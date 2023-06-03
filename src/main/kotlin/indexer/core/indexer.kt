package indexer.core

import indexer.core.FileEventType.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.withContext
import kotlin.io.path.Path
import kotlin.io.path.fileSize
import kotlin.io.path.readLines

suspend fun indexer(watchEvents: ReceiveChannel<FileEvent>, indexRequests: SendChannel<IndexRequest>) {
    for (event in watchEvents) {
        if (enableLogging.get()) println("indexer: $event")
        when (event.type) {
            CREATE, MODIFY -> handleUpdated(event, indexRequests)
            DELETE -> handleRemoved(event, indexRequests)
        }
    }
}

private suspend fun handleRemoved(event: FileEvent, indexRequests: SendChannel<IndexRequest>) {
    indexRequests.send(RemoveFileRequest(event.t, event.path, event.source))
}

private suspend fun handleUpdated(event: FileEvent, indexRequests: SendChannel<IndexRequest>) {
    withContext(Dispatchers.IO) {
        val path = Path(event.path)
        try {
            if (path.fileSize() > 10_000_000L) {
                // file to large, skip
                indexRequests.send(UpdateFileContentRequest(event.t, event.path, emptySet(), event.source))
                return@withContext
            }

            val tokens = path.readLines()
                .flatMap { tokenize(it) }
                .toSet()

            indexRequests.send(UpdateFileContentRequest(event.t, event.path, tokens, event.source))
        } catch (e: Throwable) {
            indexRequests.send(UpdateFileContentRequest(event.t, event.path, emptySet(), event.source))
//            println("Failed to read ${event.path}: $e")
        }
    }
}
