package indexer.core.internal

import com.google.common.collect.Interner
import com.google.common.collect.Interners
import indexer.core.IndexConfig
import indexer.core.IndexState
import indexer.core.IndexStatusUpdate
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.selects.select
import java.util.*
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

data class FileAddress(val path: String)

internal suspend fun index(
    cfg: IndexConfig,
    userRequests: ReceiveChannel<UserRequest>,
    indexUpdateRequests: ReceiveChannel<IndexUpdateRequest>,
    statusUpdates: ReceiveChannel<StatusUpdate>,
    indexStatusUpdate: MutableSharedFlow<IndexStatusUpdate>,
) = coroutineScope {

    val emitStatusUpdate: suspend (IndexStatusUpdate) -> Unit = { status -> indexStatusUpdate.emit(status) }
    val index = IndexStateHolder(cfg, coroutineContext, emitStatusUpdate)

    try {
        while (true) {
            select {
                statusUpdates.onReceive { event ->
                    cfg.debugLog("statusUpdates: $event")
                    when (event) {
                        WatcherDiscoveredFileDuringInitialization -> index.handleWatcherDiscoveredFileDuringInitialization()
                        WatcherStarted -> index.handleWatcherStarted()
                        is FileUpdated -> index.handleFileUpdated()
                        AllFilesDiscovered -> index.handleAllFilesDiscovered()
                        is FileSyncFailed -> index.handleFileSyncFailed(event)
                    }
                }
                userRequests.onReceive { event ->
                    cfg.debugLog("userRequests: $event")
                    when (event) {
                        is FindRequest -> index.handleFindRequest(event)
                        is StatusRequest -> index.handleStatusRequest(event)
                    }
                }
                indexUpdateRequests.onReceive { event ->
                    cfg.debugLog("indexRequests: $event")
                    when (event) {
                        is UpdateFileContentRequest -> index.handleUpdateFileContentRequest(event)
                        is RemoveFileRequest -> index.handleRemoveFileRequest(event)
                    }
                }
            }
        }
    } catch (e: Throwable) {
        index.handleException(e)
        throw e
    } finally {
        withContext(NonCancellable) {
            index.handleComplete()
        }
    }
}

@Suppress("RedundantSuspendModifier") // they are here for consistency reasons
internal class IndexStateHolder(
    private val cfg: IndexConfig,
    private val ctx: CoroutineContext,
    private val emitStatusUpdate: suspend (IndexStatusUpdate) -> Unit,
) {
    private val forwardIndex = Int2ObjectOpenHashMap<IntOpenHashSet>()
    private val reverseIndex = Int2ObjectOpenHashMap<IntOpenHashSet>()

    private var clock = 0L

    private var logicalTimeOfLastWatcherReset = 0L

    private val startTime = System.currentTimeMillis()
    private var lastRestartTime = System.currentTimeMillis()
    private var watcherStarted: Boolean = false
    private var allFilesDiscovered: Boolean = false
    private var syncCompleted: Boolean = false

    private val fileUpdateTimes = WeakHashMap<FileAddress, Long>()

    private val tokenInterner: Interner<String> = Interners.newWeakInterner()
    private var lastStringRef = 0
    private val stringRefs = WeakHashMap<String, Int>()

    private var lastFaRef = 0
    private val faRefs = WeakHashMap<String, Int>()

    private var filesDiscoveredByWatcherDuringInitialization = 0L
    private var totalFileEvents = 0L
    private var handledFileEvents = 0L

    suspend fun handleUpdateFileContentRequest(event: UpdateFileContentRequest) {
        clock++
        checkEventHappenedAfterReset(event.t) ?: return
        handleFileEventHandled()

        val fa = event.fileAddress

        checkLaterEventAlreadyHandled(event.t, fa) ?: return

        val tokens = event.tokens.map { token -> tokenInterner.intern(token) }
        val tokensArray = tokens
            .map { stringRefs.computeIfAbsent(it) { ++lastStringRef } }
            .toIntArray()
            .let { IntOpenHashSet(it) }

        val faRef = faRefs.computeIfAbsent(fa.path) { ++lastFaRef }

        forwardIndex[faRef]?.let { prevTokens ->
            prevTokens.forEach {
                reverseIndex[it]?.remove(faRef)
            }
        }

        forwardIndex[faRef] = tokensArray
        tokensArray.forEach { reverseIndex[it] = (reverseIndex[it] ?: IntOpenHashSet()).apply { add(faRef) } }
    }

    suspend fun handleRemoveFileRequest(event: RemoveFileRequest) {
        clock++
        checkEventHappenedAfterReset(event.t) ?: return
        handleFileEventHandled()

        val fa = event.fileAddress

        checkLaterEventAlreadyHandled(event.t, fa) ?: return

        val faRef = faRefs.computeIfAbsent(fa.path) { ++lastFaRef }
        forwardIndex[faRef]?.let { prevTokens ->
            prevTokens.forEach {
                reverseIndex[it]?.remove(faRef)
            }
        }
        forwardIndex.remove(faRef)
    }

    suspend fun handleStatusRequest(event: StatusRequest) {
        event.result.complete(status())
    }

    suspend fun handleFindRequest(event: FindRequest) {
        val result = event.result

        val indexContext = coroutineContext

        forwardIndex.keys.toSet()
        reverseIndex.keys.toSet()

        val flow = flow<FileAddress> {
            val consumerContext = coroutineContext
//            cfg.find(event.query, forwardIndex, reverseIndex) { indexContext.isActive && consumerContext.isActive }
//                .distinct()
//                .forEach { emit(it) }
        }
        result.complete(flow)
    }

    suspend fun handleWatcherStarted() {
        val watcherStartedTime = System.currentTimeMillis()
        cfg.debugLog("Watcher started after ${watcherStartedTime - startTime} ms!")
        watcherStarted = true
        emitStatusUpdate(IndexStatusUpdate.WatcherStarted(watcherStartedTime, status()))
    }

    suspend fun handleAllFilesDiscovered() {
        val allFilesDiscoveredTime = System.currentTimeMillis()
        allFilesDiscovered = true
        cfg.debugLog("All files discovered after ${allFilesDiscoveredTime - startTime} ms!")
        emitStatusUpdate(IndexStatusUpdate.AllFilesDiscovered(allFilesDiscoveredTime, status()))
        if (handledFileEvents == totalFileEvents) {
            emitStatusUpdate(IndexStatusUpdate.IndexInSync(allFilesDiscoveredTime, status()))
        }
    }

    suspend fun handleFileUpdated() {
        val wasInSync = allFilesDiscovered && handledFileEvents == totalFileEvents
        clock++
        totalFileEvents++
        if (wasInSync) {
            emitStatusUpdate(IndexStatusUpdate.IndexOutOfSync(System.currentTimeMillis(), status()))
        }
    }

    suspend fun handleWatcherDiscoveredFileDuringInitialization() {
        filesDiscoveredByWatcherDuringInitialization++
    }

    suspend fun handleException(e: Throwable) {
        clock++
    }

    suspend fun handleComplete() {
        clock++
        forwardIndex.clear()
        reverseIndex.clear()
    }

    suspend fun handleFileSyncFailed(event: FileSyncFailed) {
        clock++
        watcherStarted = false
        allFilesDiscovered = false
        lastRestartTime = System.currentTimeMillis()
        fileUpdateTimes.clear()
        forwardIndex.clear()
        reverseIndex.clear()
        filesDiscoveredByWatcherDuringInitialization = 0L
        totalFileEvents = 0L
        handledFileEvents = 0L
        logicalTimeOfLastWatcherReset = event.t

        emitStatusUpdate(IndexStatusUpdate.ReinitializingBecauseFileSyncFailed(System.currentTimeMillis(), event.reason))
    }

    private suspend fun handleFileEventHandled() {
        handledFileEvents++
        if (allFilesDiscovered && handledFileEvents == totalFileEvents) {
            if (!syncCompleted) {
                val syncCompletedTime = System.currentTimeMillis()
                syncCompleted = true
                cfg.debugLog("Initial sync completed after ${syncCompletedTime - startTime} ms!")
            }
            emitStatusUpdate(IndexStatusUpdate.IndexInSync(System.currentTimeMillis(), status()))
        }
    }

    private fun checkEventHappenedAfterReset(eventTime: Long): Unit? {
        if (eventTime < logicalTimeOfLastWatcherReset) return null
        return Unit
    }

    private fun checkLaterEventAlreadyHandled(eventTime: Long, fa: FileAddress): Unit? {
        val lastUpdate = fileUpdateTimes[fa] ?: 0L
        if (eventTime < lastUpdate) return null
        fileUpdateTimes[fa] = eventTime
        return Unit
    }

    private fun status() = IndexState(
        clock = clock,
        indexedFiles = forwardIndex.size,
        knownTokens = reverseIndex.size,
        watcherStarted = watcherStarted,
        allFileDiscovered = allFilesDiscovered,
        handledFileEvents = handledFileEvents,
        totalFileEvents = if (!allFilesDiscovered) {
            maxOf(totalFileEvents, filesDiscoveredByWatcherDuringInitialization)
        } else {
            totalFileEvents
        },
        startTime = startTime,
        lastRestartTime = lastRestartTime,
        isBroken = !ctx.isActive,
    )
}