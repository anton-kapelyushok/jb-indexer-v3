package indexer.core.internal

import com.google.common.collect.Interner
import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryChangeListener
import io.methvin.watcher.DirectoryWatcher
import io.methvin.watcher.hashing.FileHasher
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import org.slf4j.helpers.NOPLogger
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

internal interface Watcher {
    suspend fun watch(
        dir: Path,
        clock: AtomicLong,
        faInterner: Interner<FileAddress>,
        fileSyncEvents: SendChannel<FileSyncEvent>,
        statusUpdates: SendChannel<StatusUpdate>,
        watcherStartedLatch: CompletableDeferred<Unit>,
    ): Result<Any>
}

internal object FsWatcher : Watcher {
    override suspend fun watch(
        dir: Path,
        clock: AtomicLong,
        faInterner: Interner<FileAddress>,
        fileSyncEvents: SendChannel<FileSyncEvent>,
        statusUpdates: SendChannel<StatusUpdate>,
        watcherStartedLatch: CompletableDeferred<Unit>,
    ): Result<Any> {
        return runCatching {
            withContext(Dispatchers.IO) {
                val watcher = buildWatcher(
                    coroutineContext,
                    dir, clock, faInterner, watcherStartedLatch, fileSyncEvents, statusUpdates
                )
                invokeOnCancellation { watcher.close() }

                // watcher.watchAsync() is reading the whole directory on start and is blocking
                // this operation takes about 20s on intellij-community repository - and we want to cancel it
                // funny thing is, this operation is not interruptible by default
                // this can be worked around by checking interruption flag in fileHasher,
                // which is called on every encountered file
                val future = runInterruptible { watcher.watchAsync() }
                statusUpdates.send(StatusUpdate.WatcherStarted)
                watcherStartedLatch.complete(Unit)
                future.join()
            }
        }
    }
}

private fun buildWatcher(
    ctx: CoroutineContext,
    dir: Path,
    clock: AtomicLong,
    faInterner: Interner<FileAddress>,
    watcherStartedLatch: CompletableDeferred<Unit>,
    fileSyncEvents: SendChannel<FileSyncEvent>,
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
        if (!watcherStartedLatch.isCompleted) {
            runBlocking(ctx + Dispatchers.Unconfined) {
                statusUpdates.send(StatusUpdate.WatcherDiscoveredFileDuringInitialization)
            }
        }
        FileHasher.LAST_MODIFIED_TIME.hash(path)
    }
    .listener(object : DirectoryChangeListener {
        override fun onEvent(event: DirectoryChangeEvent) {
            if (event.isDirectory) return
            runBlocking(ctx + Dispatchers.Unconfined) {

                val t = clock.incrementAndGet()
                statusUpdates.send(StatusUpdate.FileUpdated(FileEventSource.WATCHER))

                when (event.eventType()!!) {
                    DirectoryChangeEvent.EventType.CREATE -> {
                        fileSyncEvents.send(
                            FileSyncEvent(
                                t = t,
                                fileAddress = event.path().toFile().canonicalPath.toFileAddress(faInterner),
                                source = FileEventSource.WATCHER,
                                type = FileEventType.CREATE
                            )
                        )
                    }

                    DirectoryChangeEvent.EventType.MODIFY -> {
                        fileSyncEvents.send(
                            FileSyncEvent(
                                t = t,
                                fileAddress = event.path().toFile().canonicalPath.toFileAddress(faInterner),
                                source = FileEventSource.WATCHER,
                                type = FileEventType.MODIFY
                            )
                        )
                    }

                    DirectoryChangeEvent.EventType.DELETE -> {
                        fileSyncEvents.send(
                            FileSyncEvent(
                                t = t,
                                fileAddress = event.path().toFile().canonicalPath.toFileAddress(faInterner),
                                source = FileEventSource.WATCHER,
                                type = FileEventType.DELETE
                            )
                        )
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

internal class WatcherOverflowException : RuntimeException()


private fun String.toFileAddress(interner: Interner<FileAddress>) = interner.intern(FileAddress(this))