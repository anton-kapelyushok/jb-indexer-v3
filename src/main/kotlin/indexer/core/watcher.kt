package indexer.core

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import io.methvin.watcher.hashing.FileHasher
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference


suspend fun watcher(dir: Path, outputChannel: SendChannel<WatchEvent>) = coroutineScope {
    val watcherStarted = CompletableDeferred<Unit>()
    launch { watchDir(dir, outputChannel, watcherStarted) }
    if (!rwInitialEmitFromFileHasherHackEnabled) launch { emitInitialContent(dir, outputChannel, watcherStarted) }
}

val rwInitialEmitFromFileHasherHackEnabled = false

suspend fun emitInitialContent(
    dir: Path,
    outputChannel: SendChannel<WatchEvent>,
    watcherStarted: CompletableDeferred<Unit>
) {
    watcherStarted.await()
    withContext(Dispatchers.IO) {
        val ctx = this.coroutineContext
        Files.walk(dir)
            .use { stream ->
                stream
                    .filter(Files::isRegularFile)
                    .forEach {
                        runBlocking(ctx) {
                            outputChannel.send(WatchEvent(WatchEventType.ADDED, it))
                        }
                    }
            }


        outputChannel.send(WatchEvent(WatchEventType.SYNC_COMPLETED, dir))
    }
}

suspend fun watchDir(
    dir: Path, outputChannel:
    SendChannel<WatchEvent>,
    watcherStarted: CompletableDeferred<Unit>
) {
    coroutineScope {
        val watcherHolder = AtomicReference<DirectoryWatcher>()
        val isInitializing = AtomicBoolean(true)

        val cancellationCallbackStarted = CompletableDeferred<Unit>()
        launch { // is there a primitive for it?
            try {
                cancellationCallbackStarted.complete(Unit)
                awaitCancellation()
            } finally {
                watcherHolder.get()?.close()
            }
        }
        cancellationCallbackStarted.join()

        val job = launch(Dispatchers.IO) {
            val watcherContext = this.coroutineContext
            runInterruptible {
                val watcher = DirectoryWatcher.builder()
                    .path(dir)
                    .fileHasher { path ->
                        // A hack to fast cancel watcher.build()
                        if (Thread.interrupted()) {
                            throw InterruptedException()
                        }

                        // A hack to speed up initialization process
                        if (rwInitialEmitFromFileHasherHackEnabled && isInitializing.get()) {
                            runBlocking(watcherContext) {
                                outputChannel.send(WatchEvent(WatchEventType.ADDED, path))
                            }
                        }

                        FileHasher.LAST_MODIFIED_TIME.hash(path)
                    }
                    .listener { event ->
                        if (event.isDirectory) return@listener
                        runBlocking(watcherContext) {
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
                    .build()
                watcherHolder.set(watcher)
                ensureActive()
                val f = watcher!!.watchAsync()
                isInitializing.set(false)
                runBlocking(watcherContext) {
                    outputChannel.send(WatchEvent(WatchEventType.WATCHER_STARTED, dir))
                    if (!rwInitialEmitFromFileHasherHackEnabled) watcherStarted.complete(Unit)
                }
                f.join()
            }
        }
        job.join()
    }
}

class WatcherOverflowException : RuntimeException()