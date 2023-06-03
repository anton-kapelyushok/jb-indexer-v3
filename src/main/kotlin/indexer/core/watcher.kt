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

suspend fun watcher(
    dir: Path,
    fileEvents: SendChannel<FileEvent>,
    statusUpdates: SendChannel<StatusUpdate>
) = coroutineScope {
    val clock = AtomicLong(0L)
    val watcherStartedLatch = CompletableDeferred<Unit>()
    val initialSyncCompleteLatch = CompletableDeferred<Unit>()

    launch { watch(dir, clock, fileEvents, statusUpdates, initialSyncCompleteLatch, watcherStartedLatch) }
    watcherStartedLatch.await()

    emitInitialContent(dir, clock, initialSyncCompleteLatch, fileEvents, statusUpdates)
}

suspend fun emitInitialContent(
    dir: Path,
    clock: AtomicLong,
    initialSyncCompleteLatch: CompletableDeferred<Unit>,
    outputChannel: SendChannel<FileEvent>,
    statusUpdates: SendChannel<StatusUpdate>,
) {
    withContext(Dispatchers.IO) {
        Files.walk(dir)
            .use { stream ->
                stream
                    .filter(Files::isRegularFile)
                    .forEach {
                        runBlocking(coroutineContext + Dispatchers.Unconfined) {
                            statusUpdates.send(ModificationHappened(INITIAL_SYNC))
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
        statusUpdates.send(AllFilesDiscovered)
    }
}

suspend fun watch(
    dir: Path,
    clock: AtomicLong,
    outputChannel: SendChannel<FileEvent>,
    statusUpdates: SendChannel<StatusUpdate>,
    initialSyncCompleteLatch: CompletableDeferred<Unit>,
    watcherStartedLatch: CompletableDeferred<Unit>,
) {
    withContext(Dispatchers.IO) {
        val watcher = buildWatcher(
            coroutineContext,
            dir, clock, initialSyncCompleteLatch, outputChannel, statusUpdates
        )

        invokeOnCancellation({ watcher.close() }) {
            runInterruptible {
                val f = watcher.watchAsync()
                runBlocking(coroutineContext) {
                    statusUpdates.send(WatcherStarted)
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
    outputChannel: SendChannel<FileEvent>,
    statusUpdates: SendChannel<StatusUpdate>,
): DirectoryWatcher = DirectoryWatcher.builder()
    .path(dir)
    .logger(NOPLogger.NOP_LOGGER)
    .fileHasher { path ->
        // A hack to fast cancel watcher.watchAsync()
        if (Thread.interrupted()) {
            throw InterruptedException()
        }

        // A hack to show some information during watcher initialization
        if (!initialSyncCompleteLatch.isCompleted) {
            runBlocking(ctx + Dispatchers.Unconfined) {
                statusUpdates.send(WatcherDiscoveredFileDuringInitialization)
            }
        }
        FileHasher.LAST_MODIFIED_TIME.hash(path)
    }
    .listener(object : DirectoryChangeListener {
        override fun onEvent(event: DirectoryChangeEvent) {
            if (event.isDirectory) return
            runBlocking(ctx) {
                initialSyncCompleteLatch.await()

                val t = clock.incrementAndGet()
                statusUpdates.send(ModificationHappened(WATCHER))

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