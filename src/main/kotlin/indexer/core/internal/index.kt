package indexer.core.internal

import com.google.common.collect.Interners
import indexer.core.IndexConfig
import indexer.core.IndexStatus
import indexer.core.IndexStatusUpdate
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withContext
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

data class FileAddress(val path: String)

internal suspend fun index(
    cfg: IndexConfig,
    generation: Int,
    userRequests: ReceiveChannel<UserRequest>,
    indexUpdateRequests: ReceiveChannel<IndexUpdateRequest>,
    statusUpdates: ReceiveChannel<StatusUpdate>,
    statusFlow: MutableStateFlow<IndexStatusUpdate>,
) = coroutineScope {
    val index = IndexState(cfg, coroutineContext, generation, statusFlow)
    try {
        index.handleInitializing()
        while (true) {
            select {
                statusUpdates.onReceive { event ->
                    if (cfg.enableLogging.get()) println("statusUpdates: $event")
                    when (event) {
                        AllFilesDiscovered -> index.handleAllFilesDiscovered()
                        WatcherStarted -> index.handleWatcherStarted()
                        is ModificationHappened -> index.handleModificationHappened()
                        WatcherDiscoveredFileDuringInitialization -> index.handleWatcherDiscoveredFileDuringInitialization()
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
private class IndexState(
    val cfg: IndexConfig,
    val ctx: CoroutineContext,
    val indexGeneration: Int,
    val statusFlow: MutableStateFlow<IndexStatusUpdate>
) {
    val startTime = System.currentTimeMillis()
    var watcherStartedTime: Long? = null
    var syncCompletedTime: Long? = null
    var allFilesDiscoveredTime: Long? = null

    val tokenInterner = Interners.newWeakInterner<String>()
    val fileUpdateTimes = WeakHashMap<FileAddress, Long>()

    val forwardIndex = ConcurrentHashMap<FileAddress, MutableSet<String>>()
    val reverseIndex = ConcurrentHashMap<String, MutableSet<FileAddress>>()

    var filesDiscoveredByWatcherDuringInitialization = 0L
    var totalModifications = 0L
    var handledModifications = 0L

    var exception: Throwable? = null

    suspend fun handleUpdateFileContentRequest(event: UpdateFileContentRequest) {
        updateModificationsCounts()

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
        updateModificationsCounts()
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
        emitStatusUpdate(IndexStatusUpdate.WatcherStarted(status()))
    }

    suspend fun handleAllFilesDiscovered() {
        allFilesDiscoveredTime = System.currentTimeMillis()
        if (cfg.enableLogging.get()) println("All files discovered after ${allFilesDiscoveredTime!! - startTime} ms!")
        emitStatusUpdate(IndexStatusUpdate.AllFilesDiscovered(status()))
    }

    suspend fun handleModificationHappened() {
        totalModifications++
    }

    suspend fun handleWatcherDiscoveredFileDuringInitialization() {
        filesDiscoveredByWatcherDuringInitialization++
    }

    suspend fun handleException(e: Throwable) {
        exception = e
        emitStatusUpdate(IndexStatusUpdate.IndexFailed(e))
    }

    suspend fun handleInitializing() {
        emitStatusUpdate(IndexStatusUpdate.Initializing)
    }

    suspend fun handleComplete() {
        forwardIndex.clear()
        reverseIndex.clear()
        emitStatusUpdate(
            IndexStatusUpdate.Terminated(
                exception ?: IllegalStateException("Index terminated without exception?")
            )
        )
    }

    private suspend fun updateModificationsCounts() {
        handledModifications++
        if (allFilesDiscoveredTime != null && syncCompletedTime == null && handledModifications == totalModifications) {
            syncCompletedTime = System.currentTimeMillis()
            if (cfg.enableLogging.get()) println("Initial sync completed after ${syncCompletedTime!! - startTime} ms!")
            statusFlow.emit(IndexStatusUpdate.InitialFileSyncCompleted(status()))
        }
    }

    private fun emitStatusUpdate(status: IndexStatusUpdate) {
        statusFlow.value = status
    }

    private fun checkUpdateTime(eventTime: Long, fa: FileAddress): Unit? {
        val lastUpdate = fileUpdateTimes[fa] ?: 0L
        if (lastUpdate > eventTime) return null
        fileUpdateTimes[fa] = eventTime
        return Unit
    }

    private fun status() = IndexStatus(
        indexedFiles = forwardIndex.size,
        knownTokens = reverseIndex.size,
        watcherStartTime = watcherStartedTime?.let { it - startTime },
        initialSyncTime = syncCompletedTime?.let { it - startTime },
        handledFileModifications = handledModifications,
        totalFileModifications = if (allFilesDiscoveredTime == null) {
            maxOf(
                totalModifications,
                filesDiscoveredByWatcherDuringInitialization
            )
        } else {
            totalModifications
        },
        isBroken = !ctx.isActive,
        indexGeneration = indexGeneration,
        exception = exception
    )
}