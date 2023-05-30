package indexer.core

import kotlinx.coroutines.CompletableDeferred
import java.nio.file.Path

enum class WatchEventType {
    MODIFIED, ADDED, REMOVED, INITIAL_SYNC_COMPLETED
}

data class WatchEvent(
    val type: WatchEventType,
    val path: Path,
)

sealed interface IndexRequest

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
    val possibleResults: CompletableDeferred<List<FileAddress>>
) : IndexRequest


data class StatusResult(
    val indexedFiles: Int,
    val knownTokens: Int,
    val initialSyncTime: Long?,
)

data class StatusRequest(
    val result: CompletableDeferred<StatusResult>
) : IndexRequest

object InitialSyncCompletedMessage : IndexRequest