package indexer.core.internal

import indexer.core.IndexConfig
import indexer.core.IndexState
import indexer.core.IndexStatusUpdate
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import it.unimi.dsi.fastutil.ints.IntArrayList
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.selects.select
import java.util.*
import java.util.concurrent.ConcurrentHashMap
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
                        is FindFilesByTokenRequest -> index.handleFindFileByTokenRequest(event)
                        is StatusRequest -> index.handleStatusRequest(event)
                        is FindTokensMatchingPredicateRequest -> index.handleFindTokensMatchingPredicate(event)
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

    private var reverseIndex = ConcurrentHashMap<String, IntArrayList>()

    private var clock = 0L

    private var logicalTimeOfLastWatcherReset = 0L

    private val startTime = System.currentTimeMillis()
    private var lastRestartTime = System.currentTimeMillis()
    private var watcherStarted: Boolean = false
    private var allFilesDiscovered: Boolean = false
    private var syncCompleted: Boolean = false

    private val fileUpdateTimes = WeakHashMap<FileAddress, Long>()

    private var aliveEntries = 0
    private var totalEntries = 0

    private var lastFaRef = 0
    private var fileAddressByFileRef = mutableMapOf<Int, FileAddress>()
    private var fileRefByFileAddress = mutableMapOf<FileAddress, Int>()
    private var entriesCountByFileRef = Int2IntOpenHashMap()

    private var filesDiscoveredByWatcherDuringInitialization = 0L
    private var totalFileEvents = 0L
    private var handledFileEvents = 0L

    suspend fun handleUpdateFileContentRequest(event: UpdateFileContentRequest) {
        handleFileSyncEvent(event.t, event.fileAddress) ?: return
        val fa = event.fileAddress

        val prevEntriesCount = removePreviousFileRef(fa)

        val faRef = lastFaRef++
        fileAddressByFileRef[faRef] = fa
        fileRefByFileAddress[fa] = faRef
        entriesCountByFileRef[faRef] = event.tokens.size

        aliveEntries = aliveEntries - prevEntriesCount + event.tokens.size
        totalEntries += event.tokens.size

        event.tokens.forEach { reverseIndex[it] = (reverseIndex[it] ?: IntArrayList(1)).apply { add(faRef) } }

        if (totalEntries != 0 && aliveEntries.toDouble() / totalEntries < 0.6) {
            println("compacting")
            compact()
        }
    }

    suspend fun handleRemoveFileRequest(event: RemoveFileRequest) {
        handleFileSyncEvent(event.t, event.fileAddress) ?: return

        val fa = event.fileAddress

        val prevEntriesCount = removePreviousFileRef(fa)
        aliveEntries -= prevEntriesCount
    }

    suspend fun handleStatusRequest(event: StatusRequest) {
        event.result.complete(status())
    }

    suspend fun handleFindFileByTokenRequest(event: FindFilesByTokenRequest) {
        val result = event.result

        result.complete(
            (reverseIndex[event.query]?.mapNotNull { fileAddressByFileRef[it] } ?: listOf())
        )
    }


    suspend fun handleFindTokensMatchingPredicate(event: FindTokensMatchingPredicateRequest) {
        val result = event.result
        result.complete(
            (reverseIndex.keys.filter { event.matches(it) })
        )
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
        reverseIndex.clear()
    }

    suspend fun handleFileSyncFailed(event: FileSyncFailed) {
        clock++
        watcherStarted = false
        allFilesDiscovered = false
        lastRestartTime = System.currentTimeMillis()
        fileUpdateTimes.clear()
        reverseIndex.clear()
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

    private fun removePreviousFileRef(fa: FileAddress): Int {
        val prevRef = fileRefByFileAddress[fa]
        var prevEntriesCount = 0
        if (prevRef != null) {
            fileAddressByFileRef.remove(prevRef)
            fileRefByFileAddress.remove(fa)
            prevEntriesCount = entriesCountByFileRef.remove(prevRef)
        }
        return prevEntriesCount
    }

    private suspend fun compact() {
        val remappedKeys = mutableMapOf<Int, Int>()
        var lastKey = 1
        val start = System.currentTimeMillis()
        fileAddressByFileRef.keys.forEach { key ->
            remappedKeys[key] = lastKey++
        }
        println("1 ${System.currentTimeMillis() - start}ms")

        lastFaRef = lastKey
        fileAddressByFileRef = fileAddressByFileRef.mapKeys { (k, _) -> remappedKeys[k]!! }.toMutableMap()
        println("2 ${System.currentTimeMillis() - start}ms")
        fileRefByFileAddress = fileRefByFileAddress.mapValues { (_, v) -> remappedKeys[v]!! }.toMutableMap()
        println("3 ${System.currentTimeMillis() - start}ms")
        entriesCountByFileRef = entriesCountByFileRef.mapKeysTo(Int2IntOpenHashMap()) { (k, _) -> remappedKeys[k] }
        println("4 ${System.currentTimeMillis() - start}ms")


        val copyOfKeys = reverseIndex.keys.toList()
        println("5 ${System.currentTimeMillis() - start}ms")
        val chunksCount = Runtime.getRuntime().availableProcessors()
        val chunkSize = copyOfKeys.size / chunksCount + 1
        println("available processors $chunksCount")
        coroutineScope {
            for (i in 0 until chunksCount) {
                launch(Dispatchers.Default) {
                    for (j in chunkSize * i until minOf(chunkSize * (i + 1), copyOfKeys.size)) {
                        val key = copyOfKeys[j]
                        val newData = reverseIndex[key]!!.mapNotNullTo(IntArrayList()) { remappedKeys[it] }
                        if (newData.isEmpty) {
                            reverseIndex.remove(key)
                        } else {
                            reverseIndex[key] = newData
                        }
                    }
                }
            }
        }

        totalEntries = aliveEntries
        println("6 ${System.currentTimeMillis() - start}ms")
    }

    private fun status() = IndexState(
        clock = clock,
        indexedFiles = 0,
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