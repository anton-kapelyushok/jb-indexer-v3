package indexer.core

import indexer.core.internal.FileAddress
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.flow.Flow
import java.util.concurrent.atomic.AtomicBoolean

interface Index : Deferred<Any?> {
    suspend fun findFileCandidates(query: String): Flow<FileAddress>
    suspend fun status(): IndexStatus
    suspend fun statusFlow(): Flow<IndexStateUpdate>
}

interface SearchEngine : Deferred<Any?> {
    suspend fun indexStatus(): IndexStatus
    suspend fun indexStatusUpdates(): Flow<IndexStateUpdate>
    suspend fun find(query: String): Flow<IndexSearchResult>
}

interface IndexConfig {
    val enableLogging: AtomicBoolean

    val enableWatcher: Boolean

    // transforms line into tokens for index to store
    fun tokenize(line: String): List<String>

    // returns possible files that match query
    fun find(
        query: String,
        forwardIndex: Map<FileAddress, Set<String>>,
        reverseIndex: Map<String, Set<FileAddress>>,
        isActive: () -> Boolean,
    ): Sequence<FileAddress>

    // determines if line matches query
    fun matches(line: String, query: String): Boolean

    // watcher is restarted on error, exception is passed to indexStatusUpdates flow
    // however this flow has limited buffer and error might be lost
    // you can use this function to make sure you catch this exception
    suspend fun handleWatcherError(e: Throwable)
}

data class IndexSearchResult(val path: String, val lineNo: Int, val line: String)

data class IndexStatus(
    val handledEventsCount: Long,

    val indexedFiles: Int,
    val knownTokens: Int,
    val watcherStartTime: Long?,
    val initialSyncTime: Long?,
    val handledFileEvents: Long,
    val totalFileEvents: Long,
    val isBroken: Boolean,
) {
    companion object {
        fun broken() = IndexStatus(
            handledEventsCount = -1L,
            isBroken = true,

            indexedFiles = 0,
            knownTokens = 0,
            watcherStartTime = null,
            initialSyncTime = null,
            handledFileEvents = 0L,
            totalFileEvents = 0L,
        )
    }
}

sealed interface IndexStateUpdate {
    object Initial : IndexStateUpdate
    object Initializing : IndexStateUpdate

    data class WatcherStarted(val status: IndexStatus) : IndexStateUpdate
    data class AllFilesDiscovered(val status: IndexStatus) : IndexStateUpdate

    data class IndexInSync(val status: IndexStatus) : IndexStateUpdate
    data class IndexOutOfSync(val status: IndexStatus) : IndexStateUpdate

    data class ReinitializingBecauseWatcherFailed(val reason: Throwable) : IndexStateUpdate

    data class Failed(val reason: Throwable) : IndexStateUpdate
}
