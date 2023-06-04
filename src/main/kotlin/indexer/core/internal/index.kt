package indexer.core.internal

import indexer.core.IndexConfig
import indexer.core.StatusResult
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
    generation: Int,
    userRequests: ReceiveChannel<UserRequest>,
    indexUpdateRequests: ReceiveChannel<IndexUpdateRequest>,
    statusUpdates: ReceiveChannel<StatusUpdate>,
    statusFlow: MutableSharedFlow<StatusResult>,
) = coroutineScope {
    val index = IndexState(cfg, coroutineContext, generation, statusFlow)

    try {
        index.init()
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
            index.handleCopmlete()
        }
    }
}

private class IndexState(
    val cfg: IndexConfig,
    val ctx: CoroutineContext,
    val generation: Int,
    val statusFlow: MutableSharedFlow<StatusResult>
) {
    val startTime = System.currentTimeMillis()
    var watcherStartedTime: Long? = null
    var syncCompletedTime: Long? = null
    var allFilesDiscoveredTime: Long? = null

    // TODO: check if it actually works
    val fas = WeakHashMap<String, FileAddress>()
    val interner = WeakHashMap<String, String>()
    val fileUpdateTimes = WeakHashMap<FileAddress, Long>()

    val forwardIndex = ConcurrentHashMap<FileAddress, MutableSet<String>>()
    val reverseIndex = ConcurrentHashMap<String, MutableSet<FileAddress>>()

    var filesDiscoveredByWatcherDuringInitialization = 0L
    var totalModifications = 0L
    var handledModifications = 0L

    var exception: Throwable? = null

    suspend fun handleUpdateFileContentRequest(event: UpdateFileContentRequest) {
        updateModificationsCounts()

        val path = event.path
        val fa = fas.computeIfAbsent(path) { FileAddress(it) }

        checkUpdateTime(event.t, fa) ?: return

        val tokens = event.tokens.map { token -> interner.computeIfAbsent(token) { it } }

        forwardIndex[fa]?.let { prevTokens ->
            prevTokens.forEach { reverseIndex[it]?.remove(fa) }
        }

        forwardIndex[fa] = ConcurrentHashMap.newKeySet<String>().apply { addAll(tokens) }
        tokens.forEach { reverseIndex[it] = (reverseIndex[it] ?: ConcurrentHashMap.newKeySet()).apply { add(fa) } }
    }

    suspend fun handleRemoveFileRequest(event: RemoveFileRequest) {
        updateModificationsCounts()
        val path = event.path
        val fa = fas.computeIfAbsent(path) { FileAddress(it) }

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
        statusFlow.emit(status())
    }

    suspend fun handleAllFilesDiscovered() {
        allFilesDiscoveredTime = System.currentTimeMillis()
        if (cfg.enableLogging.get()) println("All files discovered after ${allFilesDiscoveredTime!! - startTime} ms!")
        statusFlow.emit(status())
    }

    suspend fun handleModificationHappened() {
        totalModifications++
    }

    suspend fun handleWatcherDiscoveredFileDuringInitialization() {
        filesDiscoveredByWatcherDuringInitialization++
    }

    suspend fun handleException(e: Throwable) {
        exception = e
    }

    suspend fun init() {
        statusFlow.emit(status())
    }

    suspend fun handleCopmlete() {
        forwardIndex.clear()
        reverseIndex.clear()
        statusFlow.emit(status())
    }

    private suspend fun updateModificationsCounts() {
        handledModifications++
        if (allFilesDiscoveredTime != null && syncCompletedTime == null && handledModifications == totalModifications) {
            syncCompletedTime = System.currentTimeMillis()
            if (cfg.enableLogging.get()) println("Initial sync completed after ${syncCompletedTime!! - startTime} ms!")
            statusFlow.emit(status())
        }
    }

    private fun checkUpdateTime(eventTime: Long, fa: FileAddress): Unit? {
        val lastUpdate = fileUpdateTimes[fa] ?: 0L
        if (lastUpdate > eventTime) return null
        fileUpdateTimes[fa] = eventTime
        return Unit
    }

    private fun status() = StatusResult(
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
        generation = generation,
        exception = exception
    )
}