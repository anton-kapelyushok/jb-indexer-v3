package indexer.core

import indexer.core.internal.FileAddress
import indexer.core.internal.IndexManager
import indexer.core.internal.syncFs
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Semaphore
import java.nio.file.Path

fun CoroutineScope.launchIndex(
    dir: Path,
    cfg: IndexConfig,
): Index {
    val statusFlow = MutableSharedFlow<IndexStatusUpdate>(
        replay = 16, // is enough for everyone
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )
    statusFlow.tryEmit(IndexStatusUpdate.Initializing(System.currentTimeMillis()))

    val indexManager = IndexManager(
        emitStatusUpdate = { statusFlow.tryEmit(it) },
        enableDebugLog = cfg.enableLogging
    )

    val ioSemaphore = Semaphore(4)

    val deferred = async(Dispatchers.Default + CoroutineName("launchIndex")) {
        launch(CoroutineName("watcher")) { syncFs(cfg, dir, indexManager) }
    }

    deferred.invokeOnCompletion {
        statusFlow.tryEmit(
            IndexStatusUpdate.Failed(
                System.currentTimeMillis(),
                it
                    ?: IllegalStateException("Index terminated without exception?")
            )
        )
    }

    return object : Index, Deferred<Any?> by deferred {
        override suspend fun state(): IndexState {
            return withIndexContext {
                indexManager.handleStatusRequest()
            } ?: IndexState.broken()
        }

        override suspend fun statusFlow(): Flow<IndexStatusUpdate> {
            return flow {
                statusFlow
                    .onEach { emit(it) }
                    .takeWhile { it !is IndexStatusUpdate.Failed }
                    .collect()
            }
        }

        override suspend fun compact() {
            return withIndexContext {
                indexManager.handleCompactRequest()
            } ?: Unit
        }

        override suspend fun findFilesByToken(query: String): List<FileAddress> {
            return withIndexContext {
                indexManager.handleFindFileByTokenRequest(query)
            } ?: listOf()
        }

        override suspend fun findTokensMatchingPredicate(predicate: (token: String) -> Boolean): List<String> {
            return withIndexContext {
                indexManager.handleFindTokensMatchingPredicateRequest(predicate)
            } ?: listOf()
        }

        // future.await() may get stuck if index gets canceled while message is inflight
        private suspend fun <T : Any> withIndexContext(
            block: suspend CoroutineScope.() -> T
        ): T? = coroutineScope {
            try {
                withContext(deferred) {
                    block()
                }
            } catch (e: CancellationException) {
                this@coroutineScope.ensureActive()
                null
            }
        }
    }
}
