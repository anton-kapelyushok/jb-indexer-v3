package indexer.core

import indexer.core.FileEventSource.INITIAL_SYNC
import indexer.core.FileEventSource.WATCHER
import indexer.core.FileEventType.*
import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryChangeListener
import io.methvin.watcher.DirectoryWatcher
import io.methvin.watcher.hashing.FileHasher
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import org.slf4j.helpers.NOPLogger
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

suspend fun watcher(dir: Path, outputChannel: SendChannel<FileEvent>) = coroutineScope {
    val clock = AtomicLong(0L)
    val watcherStartedLatch = CompletableDeferred<Unit>()
    val initialSyncCompleteLatch = CompletableDeferred<Unit>()

    launch { watch(dir, clock, outputChannel, initialSyncCompleteLatch, watcherStartedLatch) }
    watcherStartedLatch.await()

    emitInitialContent(dir, clock, initialSyncCompleteLatch, outputChannel)
}

suspend fun emitInitialContent(
    dir: Path,
    clock: AtomicLong,
    initialSyncCompleteLatch: CompletableDeferred<Unit>,
    outputChannel: SendChannel<FileEvent>
) {
    withContext(Dispatchers.IO) {
        Files.walk(dir)
            .use { stream ->
                stream
                    .filter(Files::isRegularFile)
                    .forEach {
                        // why is it faster with Unconfined?
                        runBlocking(coroutineContext + Dispatchers.Unconfined) {
                            outputChannel.send(
                                FileEvent(
                                    clock.incrementAndGet(),
                                    it.toFile().canonicalPath,
                                    INITIAL_SYNC,
                                    CREATE,
                                )
                            )
                        }
                    }
            }

        initialSyncCompleteLatch.complete(Unit)
        // TODO
//        outputChannel.send(WatchEvent(WatchEventType.SYNC_COMPLETED, dir))
    }
}

suspend fun watch(
    dir: Path,
    clock: AtomicLong,
    outputChannel: SendChannel<FileEvent>,
    initialSyncCompleteLatch: CompletableDeferred<Unit>,
    watcherStartedLatch: CompletableDeferred<Unit>
) {
    withContext(Dispatchers.IO) {
        val watcher = buildWatcher(coroutineContext, dir, clock, initialSyncCompleteLatch, outputChannel)

        invokeOnCancellation({ watcher.close() }) {
            runInterruptible {
                val f = watcher.watchAsync()
                runBlocking(coroutineContext) {
                    // TODO
//                    outputChannel.send(WatchEvent(WatchEventType.WATCHER_STARTED, dir))
                    watcherStartedLatch.complete(Unit)
                }
                f.join()
            }
        }
    }
}

private fun buildWatcher(
    ctx: CoroutineContext,
    dir: Path,
    clock: AtomicLong,
    initialSyncCompleteLatch: CompletableDeferred<Unit>,
    outputChannel: SendChannel<FileEvent>
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
                initialSyncCompleteLatch.await()

                val t = clock.incrementAndGet()

                when (event.eventType()!!) {
                    DirectoryChangeEvent.EventType.CREATE -> {
                        outputChannel.send(
                            FileEvent(t, event.path().toFile().canonicalPath, WATCHER, CREATE)
                        )
                    }

                    DirectoryChangeEvent.EventType.MODIFY -> {
                        outputChannel.send(FileEvent(t, event.path().toFile().canonicalPath, WATCHER, MODIFY))
                    }

                    DirectoryChangeEvent.EventType.DELETE -> {
                        outputChannel.send(FileEvent(t, event.path().toFile().canonicalPath, WATCHER, DELETE))
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