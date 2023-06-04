package indexer.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.flow.Flow
import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicBoolean

interface Index : Deferred<Any?> {
    suspend fun findFileCandidates(query: String): Flow<FileAddress>
    suspend fun status(): StatusResult
    suspend fun statusFlow(): Flow<StatusResult>
    fun config(): IndexConfig
}

interface SearchEngine : Deferred<Any?> {
    suspend fun indexStatus(): StatusResult
    suspend fun indexStatusFlow(): Flow<StatusResult>
    suspend fun find(query: String): Flow<SearchResult>
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

    // determines line matches query
    fun matches(line: String, query: String): Boolean
}

data class SearchResult(val path: String, val lineNo: Int, val line: String)

data class StatusResult(
    val indexedFiles: Int,
    val knownTokens: Int,
    val watcherStartTime: Long?,
    val initialSyncTime: Long?,
    val handledFileModifications: Long,
    val totalFileModifications: Long,
    val isBroken: Boolean,
    val generation: Int,
    val exception: Throwable?,
) {
    companion object {
        fun broken() = StatusResult(
            isBroken = true,

            indexedFiles = 0,
            knownTokens = 0,
            watcherStartTime = null,
            initialSyncTime = null,
            handledFileModifications = 0L,
            totalFileModifications = 0L,
            generation = 0,
            exception = null,
        )
    }
}

internal enum class FileEventSource {
    INITIAL_SYNC,
    WATCHER,
}

internal enum class FileEventType {
    CREATE,
    DELETE,
    MODIFY,
}

internal data class FileEvent(
    val t: Long,
    val path: String,
    val source: FileEventSource,
    val type: FileEventType,
)

internal sealed interface StatusUpdate
internal object WatcherStarted : StatusUpdate
internal object AllFilesDiscovered : StatusUpdate
internal object WatcherDiscoveredFileDuringInitialization: StatusUpdate
internal data class ModificationHappened(val source: FileEventSource) : StatusUpdate

internal sealed interface IndexUpdateRequest

internal data class UpdateFileContentRequest(
    val t: Long,
    val path: String,
    val tokens: Set<String>,
    val source: FileEventSource,
) : IndexUpdateRequest {
    override fun toString(): String {
        return "UpdateFileContentRequest($path)"
    }
}

internal data class RemoveFileRequest(
    val t: Long,
    val path: String,
    val source: FileEventSource,
) : IndexUpdateRequest

internal sealed interface UserRequest

internal data class FindRequest(
    val query: String,
    val result: CompletableDeferred<Flow<FileAddress>>,
) : UserRequest

internal data class StatusRequest(
    val result: CompletableDeferred<StatusResult>
) : UserRequest


internal data class SearchInFileRequest(
    val fa: FileAddress,
    val query: String,
    val result: CompletableDeferred<List<SearchResult>>
)