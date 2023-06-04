package indexer.core.internal

import indexer.core.IndexSearchResult
import indexer.core.IndexStatus
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.Flow

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
    val fileAddress: FileAddress,
    val source: FileEventSource,
    val type: FileEventType,
)

internal sealed interface StatusUpdate
internal object WatcherStarted : StatusUpdate
internal data class WatcherFailed(val reason: Throwable) : StatusUpdate
internal object AllFilesDiscovered : StatusUpdate
internal object WatcherDiscoveredFileDuringInitialization : StatusUpdate
internal data class FileUpdated(val source: FileEventSource) : StatusUpdate

internal sealed interface IndexUpdateRequest

internal data class UpdateFileContentRequest(
    val t: Long,
    val fileAddress: FileAddress,
    val tokens: Set<String>,
    val source: FileEventSource,
) : IndexUpdateRequest

internal data class RemoveFileRequest(
    val t: Long,
    val fileAddress: FileAddress,
    val source: FileEventSource,
) : IndexUpdateRequest

internal sealed interface UserRequest

internal data class FindRequest(
    val query: String,
    val result: CompletableDeferred<Flow<FileAddress>>,
) : UserRequest

internal data class StatusRequest(
    val result: CompletableDeferred<IndexStatus>
) : UserRequest

internal data class SearchInFileRequest(
    val fa: FileAddress,
    val query: String,
    val result: CompletableDeferred<List<IndexSearchResult>>
)