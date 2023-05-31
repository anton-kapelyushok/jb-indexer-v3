package indexer.core

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryChangeListener
import io.methvin.watcher.DirectoryWatcher
import io.methvin.watcher.hashing.FileHasher
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import org.slf4j.helpers.NOPLogger
import java.nio.file.Files
import java.nio.file.Path
import kotlin.coroutines.CoroutineContext

suspend fun watcher(dir: Path, outputChannel: SendChannel<WatchEvent>) = coroutineScope {
    val watcherStarted = CompletableDeferred<Unit>()
    launch { watch(dir, outputChannel, watcherStarted) }
    watcherStarted.await()
    emitInitialContent(dir, outputChannel)
}

suspend fun emitInitialContent(
    dir: Path,
    outputChannel: SendChannel<WatchEvent>
) {
    var i = 0
    withContext(Dispatchers.IO) {
        Files.walk(dir)
            .use { stream ->
                stream
                    .filter(Files::isRegularFile)
                    .forEach {
                        runBlocking(coroutineContext) {
                            outputChannel.send(WatchEvent(WatchEventType.ADDED, it))
                        }
                    }
            }

        outputChannel.send(WatchEvent(WatchEventType.SYNC_COMPLETED, dir))
    }
}

suspend fun watch(
    dir: Path,
    outputChannel: SendChannel<WatchEvent>,
    watcherStarted: CompletableDeferred<Unit>
) {
    withContext(Dispatchers.IO) {
        val watcher = buildWatcher(coroutineContext, dir, outputChannel)
        withCancellationCallback({ watcher.close() }) {
            runInterruptible {
                val f = watcher.watchAsync()
                runBlocking(coroutineContext) {
                    outputChannel.send(WatchEvent(WatchEventType.WATCHER_STARTED, dir))
                    watcherStarted.complete(Unit)
                }
                f.join()
            }
        }
    }
}

private fun buildWatcher(
    ctx: CoroutineContext,
    dir: Path,
    outputChannel: SendChannel<WatchEvent>
): DirectoryWatcher = DirectoryWatcher.builder()
    .path(dir)
    .logger(NOPLogger.NOP_LOGGER)
    .fileHasher { path ->
        // A hack to fast cancel watcher.watchAsync()
        if (Thread.interrupted()) {
            throw InterruptedException()
        }

        FileHasher.LAST_MODIFIED_TIME.hash(path)
    }
    .listener(object : DirectoryChangeListener {
        override fun onEvent(event: DirectoryChangeEvent) {
            if (event.isDirectory) return
            runBlocking(ctx) {
                when (event.eventType()!!) {
                    DirectoryChangeEvent.EventType.CREATE -> {
                        outputChannel.send(WatchEvent(WatchEventType.ADDED, event.path()))
                    }

                    DirectoryChangeEvent.EventType.MODIFY -> {
                        outputChannel.send(WatchEvent(WatchEventType.MODIFIED, event.path()))
                    }

                    DirectoryChangeEvent.EventType.DELETE -> {
                        outputChannel.send(WatchEvent(WatchEventType.REMOVED, event.path()))
                    }

                    DirectoryChangeEvent.EventType.OVERFLOW -> throw WatcherOverflowException()
                }
            }
        }

        override fun onException(e: Exception) {
            throw e
        }
    })
    .build()

class WatcherOverflowException : RuntimeException()