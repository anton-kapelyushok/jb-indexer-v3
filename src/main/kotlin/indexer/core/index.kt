package indexer.core

import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.selects.select
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.coroutineContext

data class FileAddress(val path: String)

internal suspend fun index(
    cfg: IndexConfig,
    generation: Int,
    userRequests: ReceiveChannel<UserRequest>,
    indexRequests: ReceiveChannel<IndexRequest>,
    statusUpdates: ReceiveChannel<StatusUpdate>,
) = coroutineScope {
    val index = IndexState(cfg, generation)
    try {
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
                indexRequests.onReceive { event ->
                    if (cfg.enableLogging.get()) println("indexRequests: $event")

                    when (event) {
                        is UpdateFileContentRequest -> index.handleUpdateFileContentRequest(event)
                        is RemoveFileRequest -> index.handleRemoveFileRequest(event)
                    }
                }
            }
        }
    } finally {
        index.teardown()
    }
}

private class IndexState(val cfg: IndexConfig, val generation: Int) {
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

    fun handleUpdateFileContentRequest(event: UpdateFileContentRequest) {
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

    fun handleRemoveFileRequest(event: RemoveFileRequest) {
        updateModificationsCounts()
        val path = event.path
        val fa = fas.computeIfAbsent(path) { FileAddress(it) }

        checkUpdateTime(event.t, fa) ?: return

        forwardIndex[fa]?.let { prevTokens ->
            prevTokens.forEach { reverseIndex[it]?.remove(fa) }
        }
        forwardIndex.remove(fa)
    }

    fun handleStatusRequest(event: StatusRequest) {
        event.result.complete(
            StatusResult(
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
                isBroken = false,
                generation = generation
            )
        )
    }

    fun handleFindRequest(event: FindRequest) {
        val result = event.result

        val flow = flow {
            val ctx = coroutineContext
            cfg.find(event.query, forwardIndex, reverseIndex, ctx::isActive)
                .distinct()
                .forEach { emit(it) }
        }
        result.complete(flow)
    }

    fun handleWatcherStarted() {
        watcherStartedTime = System.currentTimeMillis()
        println("Watcher started after ${watcherStartedTime!! - startTime} ms!")
    }

    fun handleAllFilesDiscovered() {
        allFilesDiscoveredTime = System.currentTimeMillis()
        println("All files discovered after ${allFilesDiscoveredTime!! - startTime} ms!")
    }

    fun handleModificationHappened() {
        totalModifications++
    }

    fun handleWatcherDiscoveredFileDuringInitialization() {
        filesDiscoveredByWatcherDuringInitialization++
    }

    fun teardown() {
        forwardIndex.clear()
        reverseIndex.clear()
    }

    private fun updateModificationsCounts() {
        handledModifications++
        if (allFilesDiscoveredTime != null && syncCompletedTime == null && handledModifications == totalModifications) {
            syncCompletedTime = System.currentTimeMillis()
            println("Initial sync completed after ${syncCompletedTime!! - startTime} ms!")
        }
    }

    private fun checkUpdateTime(eventTime: Long, fa: FileAddress): Unit? {
        val lastUpdate = fileUpdateTimes[fa] ?: 0L
        if (lastUpdate > eventTime) return null
        fileUpdateTimes[fa] = eventTime
        return Unit
    }
}