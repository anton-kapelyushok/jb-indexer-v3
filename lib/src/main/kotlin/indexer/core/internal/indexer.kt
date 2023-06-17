package indexer.core.internal

import indexer.core.IndexConfig
import indexer.core.internal.FileEventType.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import java.io.IOException
import kotlin.io.path.Path
import kotlin.io.path.fileSize
import kotlin.io.path.readLines

internal suspend fun indexer(
    cfg: IndexConfig,
    event: FileSyncEvent,
    indexerManager: IndexManager,
) {
    cfg.debugLog(
        "indexer: $event"
    )
    when (event.type) {
        CREATE, MODIFY -> handleUpdated(cfg, event, indexerManager)
        DELETE -> handleRemoved(event, indexerManager)
    }
}

private suspend fun handleRemoved(event: FileSyncEvent, indexerManager: IndexManager) {
    indexerManager.handleRemoveFileRequest(
        FileUpdateRequest.RemoveFileRequest(
            event.t,
            event.fileAddress,
            event.source
        )
    )
}

private suspend fun handleUpdated(
    cfg: IndexConfig,
    event: FileSyncEvent,
    indexerManager: IndexManager,
) = cfg.ioSemaphore.withPermit {
    withContext(Dispatchers.IO) {
        val path = Path(event.fileAddress.path)
        try {
            if (path.fileSize() > 10_000_000L) {
                // file too large, skip
                indexerManager.handleUpdateFileContentRequest(
                    FileUpdateRequest.UpdateFile(
                        t = event.t,
                        fileAddress = event.fileAddress,
                        tokens = setOf(),
                        source = event.source
                    )
                )
                return@withContext
            }

            val tokens = path.readLines()
                .flatMap { cfg.tokenize(it) }
                .toSet()

            indexerManager.handleUpdateFileContentRequest(
                FileUpdateRequest.UpdateFile(
                    t = event.t,
                    fileAddress = event.fileAddress,
                    tokens = tokens,
                    source = event.source
                )
            )
        } catch (e: IOException) {
            indexerManager.handleUpdateFileContentRequest(
                FileUpdateRequest.UpdateFile(
                    t = event.t,
                    fileAddress = event.fileAddress,
                    tokens = setOf(),
                    source = event.source
                )
            )
            cfg.debugLog("Failed to read ${event.fileAddress}: $e")
        } catch (e: Throwable) {
            throw e
        }
    }
}
