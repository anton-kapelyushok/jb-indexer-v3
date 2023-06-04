package indexer.core.internal

import indexer.core.IndexConfig
import indexer.core.internal.FileEventType.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.withContext
import java.io.IOException
import kotlin.io.path.Path
import kotlin.io.path.fileSize
import kotlin.io.path.readLines

internal suspend fun indexer(
    cfg: IndexConfig,
    watchEvents: ReceiveChannel<FileEvent>,
    indexUpdateRequests: SendChannel<IndexUpdateRequest>
) {
    for (event in watchEvents) {
        if (cfg.enableLogging.get()) println("indexer: $event")
        when (event.type) {
            CREATE, MODIFY -> handleUpdated(cfg, event, indexUpdateRequests)
            DELETE -> handleRemoved(event, indexUpdateRequests)
        }
    }
}

private suspend fun handleRemoved(event: FileEvent, indexUpdateRequests: SendChannel<IndexUpdateRequest>) {
    indexUpdateRequests.send(RemoveFileRequest(event.t, event.path, event.source))
}

private suspend fun handleUpdated(
    cfg: IndexConfig,
    event: FileEvent,
    indexUpdateRequests: SendChannel<IndexUpdateRequest>
) {
    withContext(Dispatchers.IO) {
        val path = Path(event.path)
        try {
            if (path.fileSize() > 10_000_000L) {
                // file to large, skip
                indexUpdateRequests.send(UpdateFileContentRequest(event.t, event.path, emptySet(), event.source))
                return@withContext
            }

            val tokens = path.readLines()
                .flatMap { cfg.tokenize(it) }
                .toSet()

            indexUpdateRequests.send(UpdateFileContentRequest(event.t, event.path, tokens, event.source))
        } catch (e: IOException) {
            indexUpdateRequests.send(UpdateFileContentRequest(event.t, event.path, emptySet(), event.source))
            if (cfg.enableLogging.get()) println("Failed to read ${event.path}: $e")
        }
    }
}
