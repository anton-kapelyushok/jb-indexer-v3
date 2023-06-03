package indexer.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow

interface Index : Job {
    suspend fun status(): StatusResult
    suspend fun find(query: String): Flow<SearchResult>
}

data class SearchResult(val path: String, val lineNo: Int, val line: String)

data class StatusResult(
    val indexedFiles: Int,
    val knownTokens: Int,
    val watcherStartTime: Long?,
    val initialSyncTime: Long?,
    val handledFileModifications: Long,
    val totalFileModifications: Long
)

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

internal sealed interface IndexRequest

internal data class UpdateFileContentRequest(
    val t: Long,
    val path: String,
    val tokens: Set<String>,
    val source: FileEventSource,
) : IndexRequest {
    override fun toString(): String {
        return "UpdateFileContentRequest($path)"
    }
}

internal data class RemoveFileRequest(
    val t: Long,
    val path: String,
    val source: FileEventSource,
) : IndexRequest

sealed interface UserRequest {
    fun onMessageLoss() {}
}

internal data class FindTokenRequest(
    val query: String,
    val isConsumerAlive: () -> Boolean,
    val onResult: suspend (FileAddress) -> Result<Unit>,
    val onError: (Throwable) -> Unit,
    val onFinish: () -> Unit,
) : UserRequest {
    override fun onMessageLoss() {
        onError(CancellationException("Message was lost in flight"))
        onFinish()
    }
}

internal data class StatusRequest(
    val result: CompletableDeferred<StatusResult>
) : UserRequest
