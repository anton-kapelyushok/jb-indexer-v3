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
    watchEvents: ReceiveChannel<FileSyncEvent>,
    indexUpdateRequests: SendChannel<FileUpdateRequest>
) {
    for (event in watchEvents) {
        cfg.debugLog("indexer: $event")
        when (event.type) {
            CREATE, MODIFY -> handleUpdated(cfg, event, indexUpdateRequests)
            DELETE -> handleRemoved(event, indexUpdateRequests)
        }
    }
}

private suspend fun handleRemoved(event: FileSyncEvent, indexUpdateRequests: SendChannel<FileUpdateRequest>) {
    indexUpdateRequests.send(RemoveFileRequest(event.t, event.fileAddress, event.source))
}

private suspend fun handleUpdated(
    cfg: IndexConfig,
    event: FileSyncEvent,
    indexUpdateRequests: SendChannel<FileUpdateRequest>
) {
    withContext(Dispatchers.IO) {
        val path = Path(event.fileAddress.path)
        try {
            if (path.fileSize() > 10_000_000L) {
                // file too large, skip
                indexUpdateRequests.send(UpdateFileContentRequest(event.t, event.fileAddress, setOf(), event.source))
                return@withContext
            }

            val tokens = path.readLines()
                .flatMap { cfg.tokenize(it) }
                .toSet()

            indexUpdateRequests.send(UpdateFileContentRequest(event.t, event.fileAddress, tokens, event.source))
        } catch (e: IOException) {
            indexUpdateRequests.send(UpdateFileContentRequest(event.t, event.fileAddress, setOf(), event.source))
            cfg.debugLog("Failed to read ${event.fileAddress}: $e")
        } catch (e: Throwable) {
            throw e
        }
    }
}
