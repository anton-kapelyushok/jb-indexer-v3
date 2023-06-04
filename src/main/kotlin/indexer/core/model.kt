package indexer.core

import indexer.core.internal.FileAddress
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.flow.Flow
import java.util.concurrent.atomic.AtomicBoolean

interface Index : Deferred<Any?> {
    suspend fun findFileCandidates(query: String): Flow<FileAddress>
    suspend fun status(): IndexStatus
    suspend fun statusFlow(): Flow<IndexStatusUpdate>
}

interface SearchEngine : Deferred<Any?> {
    suspend fun indexStatus(): IndexStatus
    suspend fun indexStatusUpdates(): Flow<IndexStatusUpdate>
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
}

data class IndexSearchResult(val path: String, val lineNo: Int, val line: String)

data class IndexStatus(
    val indexedFiles: Int,
    val knownTokens: Int,
    val watcherStartTime: Long?,
    val initialSyncTime: Long?,
    val handledFileModifications: Long,
    val totalFileModifications: Long,
    val isBroken: Boolean,
    val indexGeneration: Int,
    val exception: Throwable?,
) {
    companion object {
        fun initial(indexGeneration: Int = 0) = IndexStatus(
            isBroken = true,

            indexedFiles = 0,
            knownTokens = 0,
            watcherStartTime = null,
            initialSyncTime = null,
            handledFileModifications = 0L,
            totalFileModifications = 0L,
            indexGeneration = indexGeneration,
            exception = null,
        )
    }
}

sealed interface IndexStatusUpdate {
    object Initial : IndexStatusUpdate
    object Initializing : IndexStatusUpdate
    data class WatcherStarted(val status: IndexStatus) : IndexStatusUpdate
    data class AllFilesDiscovered(val status: IndexStatus) : IndexStatusUpdate
    data class InitialFileSyncCompleted(val status: IndexStatus) : IndexStatusUpdate
    data class IndexFailed(val reason: Throwable) : IndexStatusUpdate
    data class Terminated(val reason: Throwable) : IndexStatusUpdate
    object Restarting : IndexStatusUpdate
}
