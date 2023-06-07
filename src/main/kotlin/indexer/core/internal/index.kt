package indexer.core.internal

import indexer.core.IndexConfig
import indexer.core.IndexState
import indexer.core.IndexStatusUpdate
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.selects.select
import java.util.*
import kotlin.coroutines.CoroutineContext

data class FileAddress(val path: String)

internal suspend fun index(
    cfg: IndexConfig,
    userRequests: ReceiveChannel<UserRequest>,
    indexUpdateRequests: ReceiveChannel<FileUpdateRequest>,
    statusUpdates: ReceiveChannel<StatusUpdate>,
    indexStatusUpdate: MutableSharedFlow<IndexStatusUpdate>,
) = coroutineScope {

    val emitStatusUpdate: suspend (IndexStatusUpdate) -> Unit = { status -> indexStatusUpdate.emit(status) }
    val index = IndexManager(cfg, coroutineContext, emitStatusUpdate)

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
                        is FindFilesByTokenRequest -> index.handleFindFileByTokenRequest(event)
                        is StatusRequest -> index.handleStatusRequest(event)
                        is FindTokensMatchingPredicateRequest -> index.handleFindTokensMatchingPredicateRequest(event)
                        is CompactRequest -> index.handleCompactRequest(event)
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
internal class IndexManager(
    private val cfg: IndexConfig,
    private val ctx: CoroutineContext,
    private val emitStatusUpdate: suspend (IndexStatusUpdate) -> Unit,
) {
    // inverted index data
    private val invertedIndex = InvertedIndex()

    // index manager data
    private var clock = 0L
    private var logicalTimeOfLastWatcherReset = 0L
    private val startTime = System.currentTimeMillis()
    private var lastRestartTime = System.currentTimeMillis()
    private var watcherStarted: Boolean = false
    private var allFilesDiscovered: Boolean = false
    private var syncCompleted: Boolean = false
    private val fileUpdateTimes = WeakHashMap<FileAddress, Long>()
    private var filesDiscoveredByWatcherDuringInitialization = 0L
    private var totalFileEvents = 0L
    private var handledFileEvents = 0L

    suspend fun handleUpdateFileContentRequest(event: UpdateFileContentRequest) {
        handleFileSyncEvent(event.t, event.fileAddress) ?: return
        invertedIndex.addOrUpdateDocument(event.fileAddress, event.tokens)
    }

    suspend fun handleRemoveFileRequest(event: RemoveFileRequest) {
        handleFileSyncEvent(event.t, event.fileAddress) ?: return
        invertedIndex.removeDocument(event.fileAddress)
    }

    suspend fun handleStatusRequest(event: StatusRequest) {
        event.result.complete(status())
    }

    suspend fun handleFindFileByTokenRequest(event: FindFilesByTokenRequest) {
        val result = event.result
        result.complete(invertedIndex.findFilesByToken(event.query))
    }

    suspend fun handleFindTokensMatchingPredicateRequest(event: FindTokensMatchingPredicateRequest) {
        val result = event.result
        result.complete(invertedIndex.findTokensMatchingPredicate(event.predicate))
    }

    suspend fun handleCompactRequest(event: CompactRequest) {
        invertedIndex.compact()
        event.result.complete(Unit)
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
        val wasInSync = status().isInSync()
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
        invertedIndex.reset()
    }

    suspend fun handleFileSyncFailed(event: FileSyncFailed) {
        clock++
        watcherStarted = false
        allFilesDiscovered = false
        lastRestartTime = System.currentTimeMillis()
        fileUpdateTimes.clear()
        invertedIndex.reset()
        filesDiscoveredByWatcherDuringInitialization = 0L
        totalFileEvents = 0L
        handledFileEvents = 0L
        logicalTimeOfLastWatcherReset = event.t

        emitStatusUpdate(
            IndexStatusUpdate.ReinitializingBecauseFileSyncFailed(
                System.currentTimeMillis(),
                event.reason
            )
        )
    }

    private suspend fun handleFileSyncEvent(t: Long, fa: FileAddress): Unit? {
        clock++
        checkEventHappenedAfterReset(t) ?: return null

        handledFileEvents++
        if (allFilesDiscovered && handledFileEvents == totalFileEvents) {
            if (!syncCompleted) {
                val syncCompletedTime = System.currentTimeMillis()
                syncCompleted = true
                cfg.debugLog("Initial sync completed after ${syncCompletedTime - startTime} ms!")
            }
            emitStatusUpdate(IndexStatusUpdate.IndexInSync(System.currentTimeMillis(), status()))
        }

        checkLaterEventAlreadyHandled(t, fa) ?: return null

        return Unit
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
        indexedFiles = invertedIndex.documentsCount,
        knownTokens = invertedIndex.tokensCount,
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