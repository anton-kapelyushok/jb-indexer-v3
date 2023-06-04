package indexer.core

import indexer.core.internal.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.nio.file.Path

fun launchIndex(
    scope: CoroutineScope,
    dir: Path,
    cfg: IndexConfig,
    generation: Int = 0,
): Index {
    val userRequests = Channel<UserRequest>()
    val indexUpdateRequests = Channel<IndexUpdateRequest>()
    val statusUpdates = Channel<StatusUpdate>(Int.MAX_VALUE)
    val fileEvents = Channel<FileEvent>(Int.MAX_VALUE)
    val statusFlow = MutableStateFlow<IndexStatusUpdate>(
        IndexStatusUpdate.Initial
    )

    val deferred = scope.async {
        launch(CoroutineName("watcher")) { watcher(cfg, dir, fileEvents, statusUpdates) }
        repeat(4) {
            launch(CoroutineName("indexer-$it")) { indexer(cfg, fileEvents, indexUpdateRequests) }
        }
        launch(CoroutineName("index")) {
            index(cfg, generation, userRequests, indexUpdateRequests, statusUpdates, statusFlow)
        }
    }

    deferred.invokeOnCompletion {
        if (statusFlow.value !is IndexStatusUpdate.Terminated) {
            statusFlow.value = IndexStatusUpdate.Terminated(
                it
                    ?: IllegalStateException("Index terminated without exception?")
            )
        }
    }

    return object : Index, Deferred<Any?> by deferred {
        override suspend fun status(): IndexStatus {
            return withIndexContext {
                val future = CompletableDeferred<IndexStatus>()
                userRequests.send(StatusRequest(future))
                future.await()
            } ?: IndexStatus.initial()
        }

        override suspend fun statusFlow(): Flow<IndexStatusUpdate> {
            return flow {
                statusFlow
                    .onEach { emit(it) }
                    .takeWhile { it !is IndexStatusUpdate.Terminated }
                    .collect()
            }
        }

        override suspend fun findFileCandidates(query: String): Flow<FileAddress> {
            return withIndexContext {
                val result = CompletableDeferred<Flow<FileAddress>>()
                val request = FindRequest(
                    query = query,
                    result = result,
                )
                userRequests.send(request)
                result.await()
            } ?: flowOf()
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
