package indexer.core

import indexer.core.internal.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.nio.file.Path

fun CoroutineScope.launchIndex(
    dir: Path,
    cfg: IndexConfig,
): Index {
    val userRequests = Channel<UserRequest>()
    val indexUpdateRequests = Channel<FileUpdateRequest>()
    val statusUpdates = Channel<StatusUpdate>(Int.MAX_VALUE)
    val fileSyncEvents = Channel<FileSyncEvent>(Int.MAX_VALUE)
    val statusFlow = MutableSharedFlow<IndexStatusUpdate>(
        replay = 16, // is enough for everyone
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )
    statusFlow.tryEmit(IndexStatusUpdate.Initializing(System.currentTimeMillis()))

    val deferred = async(CoroutineName("launchIndex")) {
        launch(CoroutineName("syncFs")) { syncFs(cfg, dir, fileSyncEvents, statusUpdates) }
        repeat(4) {
            launch(CoroutineName("indexer-$it")) { indexer(cfg, fileSyncEvents, indexUpdateRequests) }
        }
        launch(CoroutineName("index")) {
            index(cfg, userRequests, indexUpdateRequests, statusUpdates, statusFlow)
        }
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
                val future = CompletableDeferred<IndexState>()
                userRequests.send(StatusRequest(future))
                future.await()
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

        override suspend fun findFilesByToken(query: String): List<FileAddress> {
            return withIndexContext {
                val result = CompletableDeferred<List<FileAddress>>()
                val request = FindFilesByTokenRequest(
                    query = query,
                    result = result,
                )
                userRequests.send(request)
                result.await()
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
