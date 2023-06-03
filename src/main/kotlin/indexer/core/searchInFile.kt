package indexer.core

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import java.io.File

internal suspend fun searchInFile(
    cfg: IndexConfig,
    requests: ReceiveChannel<SearchInFileRequest>
) = withContext(Dispatchers.IO) {
    for (request in requests) {
        val result = request.result
        val flow = flow {
            File(request.fa.path)
                .readLines()
                .withIndex()
                .filter { (_, line) -> cfg.matches(line, request.query) }
                .map { (idx, line) -> SearchResult(request.fa.path, idx + 1, line) }
                .forEach { emit(it) }
        }

        result.complete(flow)
    }
}