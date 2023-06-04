package indexer.core.internal

import com.google.common.collect.Interner
import com.google.common.collect.Interners
import indexer.core.IndexConfig
import indexer.core.IndexStateUpdate
import indexer.core.IndexStatus
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.selects.select
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

data class FileAddress(val path: String)

internal suspend fun index(
    cfg: IndexConfig,
    userRequests: ReceiveChannel<UserRequest>,
    indexUpdateRequests: ReceiveChannel<IndexUpdateRequest>,
    statusUpdates: ReceiveChannel<StatusUpdate>,
    statusFlow: MutableSharedFlow<IndexStateUpdate>,
) = coroutineScope {

    val emitStatusUpdate: suspend (IndexStateUpdate) -> Unit = { status -> statusFlow.emit(status) }
    val index = IndexState(cfg, coroutineContext, emitStatusUpdate)

    try {
        while (true) {
            select {
                statusUpdates.onReceive { event ->
                    if (cfg.enableLogging.get()) println("statusUpdates: $event")
                    when (event) {
                        WatcherDiscoveredFileDuringInitialization -> index.handleWatcherDiscoveredFileDuringInitialization()
                        WatcherStarted -> index.handleWatcherStarted()
                        is FileUpdated -> index.handleFileUpdated()
                        AllFilesDiscovered -> index.handleAllFilesDiscovered()
                        is WatcherFailed -> index.handleWatcherFailed(event.reason)
                    }
                }
                userRequests.onReceive { event ->
                    if (cfg.enableLogging.get()) println("userRequests: $event")
                    when (event) {
                        is FindRequest -> index.handleFindRequest(event)
                        is StatusRequest -> index.handleStatusRequest(event)
                    }
                }
                indexUpdateRequests.onReceive { event ->
                    if (cfg.enableLogging.get()) println("indexRequests: $event")
                    when (event) {
                        is UpdateFileContentRequest -> index.handleUpdateFileContentRequest(event)
                        is RemoveFileRequest -> index.handleRemoveFileRequest(event)
                    }
                }
            }
        }
    } catch (e: Throwable) {
        index.handleException(e)
    } finally {
        withContext(NonCancellable) {
            index.handleComplete()
        }
    }
}

@Suppress("RedundantSuspendModifier") // they are here for consistency reasons
internal class IndexState(
    private val cfg: IndexConfig,
    private val ctx: CoroutineContext,
    private val emitStatusUpdate: suspend (IndexStateUpdate) -> Unit,
) {
    private var handledEventsCount = 0L
    private val startTime = System.currentTimeMillis()
    private var watcherStartedTime: Long? = null
    private var syncCompletedTime: Long? = null
    private var allFilesDiscoveredTime: Long? = null

    private val tokenInterner: Interner<String> = Interners.newWeakInterner()
    private val fileUpdateTimes = WeakHashMap<FileAddress, Long>()

    private val forwardIndex = ConcurrentHashMap<FileAddress, MutableSet<String>>()
    private val reverseIndex = ConcurrentHashMap<String, MutableSet<FileAddress>>()

    private var filesDiscoveredByWatcherDuringInitialization = 0L
    private var totalFileEvents = 0L
    private var handledFileEvents = 0L

    suspend fun handleUpdateFileContentRequest(event: UpdateFileContentRequest) {
        handledEventsCount++
        handleFileEventHandled()

        val fa = event.fileAddress

        checkUpdateTime(event.t, fa) ?: return

        val tokens = event.tokens.map { token -> tokenInterner.intern(token) }

        forwardIndex[fa]?.let { prevTokens ->
            prevTokens.forEach { reverseIndex[it]?.remove(fa) }
        }

        forwardIndex[fa] = ConcurrentHashMap.newKeySet<String>().apply { addAll(tokens) }
        tokens.forEach { reverseIndex[it] = (reverseIndex[it] ?: ConcurrentHashMap.newKeySet()).apply { add(fa) } }
    }

    suspend fun handleRemoveFileRequest(event: RemoveFileRequest) {
        handledEventsCount++
        handleFileEventHandled()
        val fa = event.fileAddress

        checkUpdateTime(event.t, fa) ?: return

        forwardIndex[fa]?.let { prevTokens ->
            prevTokens.forEach { reverseIndex[it]?.remove(fa) }
        }
        forwardIndex.remove(fa)
    }

    suspend fun handleStatusRequest(event: StatusRequest) {
        event.result.complete(status())
    }

    suspend fun handleFindRequest(event: FindRequest) {
        val result = event.result

        val indexContext = coroutineContext

        val flow = flow {
            val consumerContext = coroutineContext
            cfg.find(event.query, forwardIndex, reverseIndex) { indexContext.isActive && consumerContext.isActive }
                .distinct()
                .forEach { emit(it) }
        }
        result.complete(flow)
    }

    suspend fun handleWatcherStarted() {
        watcherStartedTime = System.currentTimeMillis()
        if (cfg.enableLogging.get()) println("Watcher started after ${watcherStartedTime!! - startTime} ms!")
        emitStatusUpdate(IndexStateUpdate.WatcherStarted(status()))
    }

    suspend fun handleAllFilesDiscovered() {
        allFilesDiscoveredTime = System.currentTimeMillis()
        if (cfg.enableLogging.get()) println("All files discovered after ${allFilesDiscoveredTime!! - startTime} ms!")
        emitStatusUpdate(IndexStateUpdate.AllFilesDiscovered(status()))
    }

    suspend fun handleFileUpdated() {
        val wasInSync = allFilesDiscoveredTime != null && handledFileEvents == totalFileEvents
        handledEventsCount++
        totalFileEvents++
        if (wasInSync) {
            emitStatusUpdate(IndexStateUpdate.IndexOutOfSync(status()))
        }
    }

    suspend fun handleWatcherDiscoveredFileDuringInitialization() {
        filesDiscoveredByWatcherDuringInitialization++
    }

    suspend fun handleException(e: Throwable) {
        handledEventsCount++
    }

    suspend fun handleComplete() {
        handledEventsCount++
        forwardIndex.clear()
        reverseIndex.clear()
    }

    suspend fun handleWatcherFailed(reason: Throwable) {
        handledEventsCount++
        watcherStartedTime = null
        syncCompletedTime = null
        allFilesDiscoveredTime = null
        fileUpdateTimes.clear()
        forwardIndex.clear()
        reverseIndex.clear()
        filesDiscoveredByWatcherDuringInitialization = 0L
        totalFileEvents = 0L
        handledFileEvents = 0L

        emitStatusUpdate(IndexStateUpdate.ReinitializingBecauseWatcherFailed(reason))
    }

    private suspend fun handleFileEventHandled() {
        handledFileEvents++
        if (allFilesDiscoveredTime != null && handledFileEvents == totalFileEvents) {
            if (syncCompletedTime == null) {
                syncCompletedTime = System.currentTimeMillis()
                if (cfg.enableLogging.get()) println("Initial sync completed after ${syncCompletedTime!! - startTime} ms!")
            }
            emitStatusUpdate(IndexStateUpdate.IndexInSync(status()))
        }
    }

    private fun checkUpdateTime(eventTime: Long, fa: FileAddress): Unit? {
        val lastUpdate = fileUpdateTimes[fa] ?: 0L
        if (lastUpdate > eventTime) return null
        fileUpdateTimes[fa] = eventTime
        return Unit
    }

    private fun status() = IndexStatus(
        handledEventsCount = 0L,
        indexedFiles = forwardIndex.size,
        knownTokens = reverseIndex.size,
        watcherStartTime = watcherStartedTime?.let { it - startTime },
        initialSyncTime = syncCompletedTime?.let { it - startTime },
        handledFileEvents = handledFileEvents,
        totalFileEvents = if (allFilesDiscoveredTime == null) {
            maxOf(
                totalFileEvents,
                filesDiscoveredByWatcherDuringInitialization
            )
        } else {
            totalFileEvents
        },
        isBroken = !ctx.isActive,
    )
}