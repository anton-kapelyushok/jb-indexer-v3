package indexer.core

import indexer.core.internal.FileAddress
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference

suspend fun launchResurrectingIndex(
    parentScope: CoroutineScope,
    dir: Path,
    cfg: IndexConfig,
    onIndexException: suspend (Throwable) -> Unit = {}
): Index {
    val indexRef = AtomicReference<Index>()
    val startedLatch = CompletableDeferred<Unit>()
    val statusFlow = MutableStateFlow<IndexStatusUpdate>(IndexStatusUpdate.Initial)

    val deferred = parentScope.async {
        supervisorScope {
            var indexGeneration = 1
            while (true) {
                try {
                    val index = launchIndex(this, dir, cfg, indexGeneration++)
                    startedLatch.complete(Unit)
                    indexRef.set(index)
                    index.statusFlow()
                        .filter { it !is IndexStatusUpdate.Terminated }
                        .onEach { statusFlow.value = it }
                        .collect()
                    index.await()
                } catch (e: Throwable) {
                    ensureActive()
                    statusFlow.value = IndexStatusUpdate.Restarting
                    onIndexException(e)

                    if (cfg.enableLogging.get()) {
                        println("Index failed with $e, rebuilding index!")
                        e.printStackTrace(System.out)
                        println()
                    }
                }
            }
        }
    }

    deferred.invokeOnCompletion {
        if (statusFlow.value !is IndexStatusUpdate.Terminated) {
            statusFlow.value =
                IndexStatusUpdate.Terminated(it ?: IllegalStateException("Index terminated without exception?"))
        }
    }

    startedLatch.await()

    return object : Index, Deferred<Any?> by deferred {
        override suspend fun findFileCandidates(query: String): Flow<FileAddress> {
            return indexRef.get().findFileCandidates(query)
        }

        override suspend fun status(): IndexStatus {
            return indexRef.get().status()
        }

        override suspend fun statusFlow(): Flow<IndexStatusUpdate> {
            return flow {
                statusFlow
                    .onEach { emit(it) }
                    .takeWhile { it !is IndexStatusUpdate.Terminated }
                    .collect()
            }
        }
    }
}
