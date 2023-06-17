package indexer.core.internal

import indexer.core.IndexState
import indexer.core.IndexStatusUpdate
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

data class FileAddress(val path: String)

internal class IndexManager(
    private val emitStatusUpdate: (IndexStatusUpdate) -> Unit,
    private val enableDebugLog: AtomicBoolean = AtomicBoolean(false),
    private val invertedIndex: InvertedIndex = InvertedIndex(),
) {
    private var clock = 0L
    private var logicalTimeOfLastWatcherReset = 0L
    private val startTime = System.currentTimeMillis()
    private var lastRestartTime = startTime
    private var watcherStarted: Boolean = false
    private var allFilesDiscovered: Boolean = false
    private var syncCompleted: Boolean = false
    private val fileUpdateTimes = WeakHashMap<FileAddress, Long>()
    private var filesDiscoveredByWatcherDuringInitialization = 0L
    private var totalFileEvents = 0L
    private var handledFileEvents = 0L

    val mutex = Mutex()

    suspend fun handleUpdateFileContentRequest(event: FileUpdateRequest.UpdateFile) = mutex.withLock {
        handleFileSyncEvent(event.t, event.fileAddress) ?: return
        invertedIndex.addOrUpdateDocument(event.fileAddress, event.tokens)
    }

    suspend fun handleRemoveFileRequest(event: FileUpdateRequest.RemoveFileRequest) = mutex.withLock {
        handleFileSyncEvent(event.t, event.fileAddress) ?: return
        invertedIndex.removeDocument(event.fileAddress)
    }

    suspend fun handleStatusRequest() = mutex.withLock {
        return@withLock status()
    }

    suspend fun handleFindFileByTokenRequest(query: String) = mutex.withLock {
        return@withLock invertedIndex.findFilesByToken(query)
    }

    suspend fun handleFindTokensMatchingPredicateRequest(predicate: (String) -> Boolean) =
        mutex.withLock {
            return@withLock invertedIndex.findTokensMatchingPredicate(predicate)
        }

    suspend fun handleCompactRequest() = mutex.withLock {
        invertedIndex.compact()
    }

    suspend fun handleWatcherStarted() = mutex.withLock {
        val watcherStartedTime = System.currentTimeMillis()
        debugLog("Watcher started after ${watcherStartedTime - startTime} ms!")
        watcherStarted = true
        emitStatusUpdate(IndexStatusUpdate.WatcherStarted(watcherStartedTime, status()))
    }

    suspend fun handleAllFilesDiscovered() = mutex.withLock {
        val allFilesDiscoveredTime = System.currentTimeMillis()
        allFilesDiscovered = true
        debugLog("All files discovered after ${allFilesDiscoveredTime - startTime} ms!")
        emitStatusUpdate(IndexStatusUpdate.AllFilesDiscovered(allFilesDiscoveredTime, status()))
        if (handledFileEvents == totalFileEvents) {
            emitStatusUpdate(IndexStatusUpdate.IndexInSync(allFilesDiscoveredTime, status()))
        }
    }

    suspend fun handleFileUpdated() = mutex.withLock {
        val wasInSync = status().isInSync()
        clock++
        totalFileEvents++
        if (wasInSync) {
            emitStatusUpdate(IndexStatusUpdate.IndexOutOfSync(System.currentTimeMillis(), status()))
        }
    }

    suspend fun handleWatcherDiscoveredFileDuringInitialization() = mutex.withLock {
        filesDiscoveredByWatcherDuringInitialization++
    }

    suspend fun handleComplete() = mutex.withLock {
        clock++
        invertedIndex.reset()
    }

    suspend fun handleFileSyncFailed(event: StatusUpdate.FileSyncFailed) = mutex.withLock {
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

    private fun handleFileSyncEvent(t: Long, fa: FileAddress): Unit? {
        clock++
        checkEventHappenedAfterReset(t) ?: return null

        handledFileEvents++
        if (allFilesDiscovered && handledFileEvents == totalFileEvents) {
            if (!syncCompleted) {
                val syncCompletedTime = System.currentTimeMillis()
                syncCompleted = true
                debugLog("Initial sync completed after ${syncCompletedTime - startTime} ms!")
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
        totalFileEvents = totalFileEvents,
        filesDiscoveredByWatcherDuringInitialization = filesDiscoveredByWatcherDuringInitialization,
        startTime = startTime,
        lastRestartTime = lastRestartTime,
        isBroken = false,
    )

    private fun debugLog(s: String) = if (enableDebugLog.get()) println(s) else Unit
}