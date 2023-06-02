package indexer.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import java.nio.file.Path


interface Index : Job {
    suspend fun status(): StatusResult
    suspend fun find(query: String): Flow<SearchResult>
}

data class SearchResult(val path: String, val lineNo: Int, val line: String)

enum class WatchEventType {
    MODIFIED, ADDED, REMOVED, WATCHER_STARTED, SYNC_COMPLETED
}

data class WatchEvent(
    val type: WatchEventType,
    val path: Path,
)

sealed interface IndexRequest {
    fun onMessageLoss() {}
}

data class UpdateFileContentRequest(
    val path: Path,
    val tokens: Set<String>,
) : IndexRequest {
    override fun toString(): String {
        return "UpdateFileContentRequest($path)"
    }
}

data class RemoveFileRequest(
    val path: Path,
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