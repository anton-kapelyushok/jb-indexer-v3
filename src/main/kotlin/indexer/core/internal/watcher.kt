package indexer.core.internal

import com.google.common.collect.Interner
import com.google.common.collect.Interners
import indexer.core.IndexConfig
import indexer.core.internal.FileEventSource.INITIAL_SYNC
import indexer.core.internal.FileEventSource.WATCHER
import indexer.core.internal.FileEventType.*
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

internal suspend fun watcher(
    cfg: IndexConfig,
    dir: Path,
    fileEvents: SendChannel<FileEvent>,
    statusUpdates: SendChannel<StatusUpdate>
) = coroutineScope {

    val clock = AtomicLong(0L)
    val faInterner = Interners.newWeakInterner<FileAddress>()

    while (true) {
        val watcherStartedLatch = CompletableDeferred<Unit>()
        val initialSyncCompleteLatch = CompletableDeferred<Unit>()

        val watcherDeferred = if (cfg.enableWatcher) {
            val deferred = async {
                watch(
                    dir,
                    clock,
                    faInterner,
                    fileEvents,
                    statusUpdates,
                    initialSyncCompleteLatch,
                    watcherStartedLatch
                )
            }
            watcherStartedLatch.await()
            deferred
        } else {
            CompletableDeferred() // never completes
        }

        emitInitialContent(dir, clock, faInterner, initialSyncCompleteLatch, fileEvents, statusUpdates)

        val watcherResult = watcherDeferred.await()
        val watcherException = watcherResult.exceptionOrNull()
            ?: IllegalStateException("Watcher completed without exception for some reason")

        cfg.handleWatcherError(watcherException)
        statusUpdates.send(WatcherFailed(watcherException))
    }
}

internal suspend fun emitInitialContent(
    dir: Path,
    clock: AtomicLong,
    faInterner: Interner<FileAddress>,
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
                            statusUpdates.send(FileUpdated(INITIAL_SYNC))
                            outputChannel.send(
                                FileEvent(
                                    clock.incrementAndGet(),
                                    it.toFile().canonicalPath.toFileAddress(faInterner),
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

internal suspend fun watch(
    dir: Path,
    clock: AtomicLong,
    faInterner: Interner<FileAddress>,
    outputChannel: SendChannel<FileEvent>,
    statusUpdates: SendChannel<StatusUpdate>,
    initialSyncCompleteLatch: CompletableDeferred<Unit>,
    watcherStartedLatch: CompletableDeferred<Unit>,
): Result<Any> {
    return runCatching {
        withContext(Dispatchers.IO) {
            val watcher = buildWatcher(
                coroutineContext,
                dir, clock, faInterner, initialSyncCompleteLatch, outputChannel, statusUpdates
            )
            invokeOnCancellation(this) { watcher.close() }
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
    faInterner: Interner<FileAddress>,
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
                // buffer watcher events until all files are emitted
                initialSyncCompleteLatch.await()

                val t = clock.incrementAndGet()
                statusUpdates.send(FileUpdated(WATCHER))

                when (event.eventType()!!) {
                    DirectoryChangeEvent.EventType.CREATE -> {
                        outputChannel.send(
                            FileEvent(
                                t = t,
                                fileAddress = event.path().toFile().canonicalPath.toFileAddress(faInterner),
                                source = WATCHER,
                                type = CREATE
                            )
                        )
                    }

                    DirectoryChangeEvent.EventType.MODIFY -> {
                        outputChannel.send(
                            FileEvent(
                                t = t,
                                fileAddress = event.path().toFile().canonicalPath.toFileAddress(faInterner),
                                source = WATCHER,
                                type = MODIFY
                            )
                        )
                    }

                    DirectoryChangeEvent.EventType.DELETE -> {
                        outputChannel.send(
                            FileEvent(
                                t = t,
                                fileAddress = event.path().toFile().canonicalPath.toFileAddress(faInterner),
                                source = WATCHER,
                                type = DELETE
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