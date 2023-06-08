package indexer.core.internal

import com.google.common.collect.Interner
import com.google.common.collect.Interners
import indexer.core.IndexConfig
import indexer.core.internal.FileEventSource.INITIAL_SYNC
import indexer.core.internal.FileEventType.CREATE
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.consumeEach
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.exists
import kotlin.io.path.isRegularFile
import kotlin.streams.asSequence

internal suspend fun syncFs(
    cfg: IndexConfig,
    dir: Path,
    fileSyncEvents: SendChannel<FileSyncEvent>,
    statusUpdates: SendChannel<StatusUpdate>,
    watcher: Watcher = FsWatcher,
) = coroutineScope {
    val clock = AtomicLong(0L)
    val faInterner = Interners.newWeakInterner<FileAddress>()

    while (true) {
        coroutineScope {
            launch {
                val scope = this
                val watcherStartedLatch = CompletableDeferred<Unit>()
                val watcherFileSyncEvents = Channel<FileSyncEvent>(Int.MAX_VALUE)

                if (cfg.enableWatcher) {
                    launch {
                        val watcherResult = watcher.watch(
                            dir,
                            clock,
                            faInterner,
                            watcherFileSyncEvents,
                            statusUpdates,
                            watcherStartedLatch
                        )
                        val watcherException = watcherResult.exceptionOrNull()
                            ?: IllegalStateException("Watcher completed without exception for some reason")
                        cfg.handleWatcherError(watcherException)
                        statusUpdates.send(StatusUpdate.FileSyncFailed(clock.incrementAndGet(), watcherException))
                        scope.cancel() // restart
                    }
                } else {
                    watcherStartedLatch.complete(Unit)
                }

                watcherStartedLatch.await()
                emitInitialContent(dir, cfg, clock, faInterner, fileSyncEvents, statusUpdates)
                watcherFileSyncEvents.consumeEach { fileSyncEvents.send(it) }
            }
        }
    }
}

internal suspend fun emitInitialContent(
    dir: Path,
    cfg: IndexConfig,
    clock: AtomicLong,
    faInterner: Interner<FileAddress>,
    fileSyncEvents: SendChannel<FileSyncEvent>,
    statusUpdates: SendChannel<StatusUpdate>,
) {
    withContext(Dispatchers.IO) {
        val retryCount = 10
        for (attempt in 1..retryCount) {
            try {
                Files.walk(dir)
                    .asSequence()
                    .filter { it.isRegularFile() }
                    .forEach {
                        ensureActive()
                        statusUpdates.send(StatusUpdate.FileUpdated(INITIAL_SYNC))
                        fileSyncEvents.send(
                            FileSyncEvent(
                                clock.incrementAndGet(),
                                it.toFile().canonicalPath.toFileAddress(faInterner),
                                INITIAL_SYNC,
                                CREATE,
                            )
                        )
                    }
            } catch (e: Throwable) {
                coroutineContext.ensureActive()
                if (!dir.exists()) {
                    val e1 = FileNotFoundException(dir.toFile().canonicalPath)
                    e1.addSuppressed(e)
                    cfg.handleInitialFileSyncError(e1)
                    throw e
                }

                cfg.handleInitialFileSyncError(e)
                if (attempt == retryCount) throw e

                // the usual cause is someone is deleting directory content while we are trying to index it
                // there is a good chance that it will throw again if we retry immediately
                // add a backoff to handle this
                delay((attempt - 1) * 1000L)
                continue
            }
            break
        }

        statusUpdates.send(StatusUpdate.AllFilesDiscovered)
    }
}

private fun String.toFileAddress(interner: Interner<FileAddress>) = interner.intern(FileAddress(this))