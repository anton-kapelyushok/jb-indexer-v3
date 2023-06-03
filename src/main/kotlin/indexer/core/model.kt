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

enum class FileEventSource {
    INITIAL_SYNC,
    WATCHER,
}

enum class FileEventType {
    CREATE,
    DELETE,
    MODIFY,
}

data class FileEvent(
    val t: Long,
    val path: String,
    val source: FileEventSource,
    val type: FileEventType,
)

sealed interface IndexRequest {
    fun onMessageLoss() {}
}

data class UpdateFileContentRequest(
    val t: Long,
    val path: String,
    val tokens: Set<String>,
    val source: FileEventSource,
) : IndexRequest {
    override fun toString(): String {
        return "UpdateFileContentRequest($path)"
    }
}

data class RemoveFileRequest(
    val t: Long,
    val path: String,
    val source: FileEventSource,
) : IndexRequest

data class FindTokenRequest(
    val query: String,
    val isConsumerAlive: () -> Boolean,
    val onResult: suspend (FileAddress) -> Result<Unit>,
    val onError: (Throwable) -> Unit,
    val onFinish: () -> Unit,
) : IndexRequest {
    override fun onMessageLoss() {
        onError(CancellationException("Message was lost in flight"))
        onFinish()
    }
}

data class StatusResult(
    val indexedFiles: Int,
    val knownTokens: Int,
    val watcherStartTime: Long?,
    val initialSyncTime: Long?,
)

data class StatusRequest(
    val result: CompletableDeferred<StatusResult>
) : IndexRequest

object SyncCompletedMessage : IndexRequest
object WatcherStartedMessage : IndexRequest