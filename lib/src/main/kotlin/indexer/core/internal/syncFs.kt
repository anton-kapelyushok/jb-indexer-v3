package indexer.core.internal

import com.google.common.collect.Interner
import com.google.common.collect.Interners
import indexer.core.IndexConfig
import indexer.core.internal.FileEventSource.INITIAL_SYNC
import indexer.core.internal.FileEventType.CREATE
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.sync.Semaphore
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
    indexManager: IndexManager,
    watcher: Watcher = FsWatcher,
) = coroutineScope {
    val clock = AtomicLong(0L)
    val faInterner = Interners.newWeakInterner<FileAddress>()

    suspend fun CoroutineScope.indexFile(event: FileSyncEvent) {
        val s = this
        s.launch { indexer(cfg, event, indexManager) }
    }

    while (true) {
        coroutineScope {
            val watcherStartedLatch = CompletableDeferred<Unit>()
            val watcherFileSyncEvents = Channel<FileSyncEvent>(Int.MAX_VALUE)

            val watcherDeferred = if (cfg.enableWatcher) {
                async {
                    watcher.watch(
                        dir,
                        faInterner,
                        watcherFileSyncEvents,
                        watcherStartedLatch,
                        indexManager
                    )
                }
            } else {
                watcherStartedLatch.complete(Unit)
                CompletableDeferred() // never completes
            }

            val job = launch {
                watcherStartedLatch.await()
                // emit initial file events only after watcher started
                emitInitialContent(dir, cfg, clock, faInterner, indexManager)
                // emit watcher events only after initial sync completed
                watcherFileSyncEvents.consumeEach { indexFile(it.copy(t = clock.incrementAndGet())) }
            }

            val watcherException = watcherDeferred.await().exceptionOrNull()
                ?: IllegalStateException("Watcher completed without exception for some reason")
            cfg.handleWatcherError(watcherException)
            // wait until no more events from this generation will be sent, and only then send FileSyncFailed
            job.cancelAndJoin()
            indexManager.handleFileSyncFailed(StatusUpdate.FileSyncFailed(clock.incrementAndGet(), watcherException))
        }
    }
}


internal suspend fun emitInitialContent(
    dir: Path,
    cfg: IndexConfig,
    clock: AtomicLong,
    faInterner: Interner<FileAddress>,
    indexManager: IndexManager,
) {
    suspend fun CoroutineScope.indexFile(event: FileSyncEvent) {
        val s = this
        s.launch { indexer(cfg, event, indexManager) }
    }


    withContext(Dispatchers.IO) {
        val retryCount = 10
        for (attempt in 1..retryCount) {
            try {
                Files.walk(dir)
                    .asSequence()
                    .filter { it.isRegularFile() }
                    .forEach {
                        ensureActive()
                        indexManager.handleFileUpdated()
                        indexFile(
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

        indexManager.handleAllFilesDiscovered()
    }
}

private fun String.toFileAddress(interner: Interner<FileAddress>) = interner.intern(FileAddress(this))