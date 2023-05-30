package indexer.core

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import io.methvin.watcher.hashing.FileHasher
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean


suspend fun watcher(dir: Path, outputChannel: SendChannel<WatchEvent>) = coroutineScope {
    watchDir(dir, outputChannel)
}

val rwInitialEmitFromFileHasherHackEnabled = true

suspend fun emitInitialContent(dir: Path, outputChannel: SendChannel<WatchEvent>) {
}

suspend fun watchDir(dir: Path, outputChannel: SendChannel<WatchEvent>) =
    coroutineScope {
        var watcher: DirectoryWatcher? = null
        val isInitializing = AtomicBoolean(true)
        launch {
            try {
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    watcher?.close()
                }
            }
        }
        launch(Dispatchers.IO) {
            val watcherContext = this.coroutineContext
            runInterruptible {
                watcher = DirectoryWatcher.builder()
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
                val f = watcher!!.watchAsync()
                isInitializing.set(false)
                runBlocking(watcherContext) {
                    outputChannel.send(WatchEvent(WatchEventType.INITIAL_SYNC_COMPLETED, dir))
                }
                f.join()
            }
        }
    }

class WatcherOverflowException : RuntimeException()