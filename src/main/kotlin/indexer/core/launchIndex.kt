package indexer.core

import indexer.core.internal.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference

suspend fun launchResurrectingIndex(parentScope: CoroutineScope, dir: Path, cfg: IndexConfig): Index {
    val indexRef = AtomicReference<Index>()
    val startedLatch = CompletableDeferred<Unit>()
    val statusFlow = MutableSharedFlow<StatusResult>(replay = 1)

    val job = parentScope.async {
        supervisorScope {
            var generation = 1
            while (true) {
                try {
                    val index = launchIndex(this, dir, cfg, generation++, statusFlow)
                    indexRef.set(index)
                    startedLatch.complete(Unit)
                    index.await()
                } catch (e: Throwable) {
                    ensureActive()
                    if (cfg.enableLogging.get()) {
                        println("Index failed with $e, rebuilding index!")
                        e.printStackTrace(System.out)
                        println()
                    }
                }
            }
        }
    }

    startedLatch.await()

    return object : Index, Deferred<Any?> by job {
        override suspend fun findFileCandidates(query: String): Flow<FileAddress> {
            return indexRef.get().findFileCandidates(query)
        }

        override suspend fun status(): StatusResult {
            return indexRef.get().status()
        }

        override suspend fun statusFlow(): Flow<StatusResult> {
            return statusFlow
                .takeWhile { job.isActive }
                .onCompletion { emit(StatusResult.broken()) }
        }
    }
}

fun launchIndex(
    scope: CoroutineScope,
    dir: Path,
    cfg: IndexConfig,
    generation: Int = 0,
    statusFlow: MutableSharedFlow<StatusResult> = MutableSharedFlow(replay = 1),
): Index {
    val userRequests = Channel<UserRequest>()
    val indexUpdateRequests = Channel<IndexUpdateRequest>()
    val statusUpdates = Channel<StatusUpdate>(Int.MAX_VALUE)
    val fileEvents = Channel<FileEvent>(Int.MAX_VALUE)

    val job = scope.async {
        launch(CoroutineName("watcher")) { watcher(cfg, dir, fileEvents, statusUpdates) }
        repeat(4) {
            launch(CoroutineName("indexer-$it")) { indexer(cfg, fileEvents, indexUpdateRequests) }
        }
        launch(CoroutineName("index")) {
            index(cfg, generation, userRequests, indexUpdateRequests, statusUpdates, statusFlow)
        }
    }

    job.invokeOnCompletion {
        userRequests.cancel(it?.let { CancellationException(it.message, it) })
        indexUpdateRequests.cancel(it?.let { CancellationException(it.message, it) })
        statusUpdates.cancel(it?.let { CancellationException(it.message, it) })
        fileEvents.cancel(it?.let { CancellationException(it.message, it) })
    }

    return object : Index, Deferred<Any?> by job {
        override suspend fun status(): StatusResult {
            return withIndexContext {
                val future = CompletableDeferred<StatusResult>()
                userRequests.send(StatusRequest(future))
                future.await()
            } ?: StatusResult.broken()
        }

        override suspend fun statusFlow(): Flow<StatusResult> {
            return statusFlow.takeWhile { job.isActive }.onCompletion { emit(StatusResult.broken()) }
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
                withContext(job) {
                    block()
                }
            } catch (e: CancellationException) {
                this@coroutineScope.ensureActive()
                null
            }
        }
    }
}

