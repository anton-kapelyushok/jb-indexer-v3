package indexer.core

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference

suspend fun launchResurrectingIndex(parentScope: CoroutineScope, dir: Path, cfg: IndexConfig): Index {
    val indexRef = AtomicReference<Index>()
    val startedLatch = CompletableDeferred<Unit>()
    val statusFlow = MutableStateFlow(StatusResult.broken())

    val job = parentScope.async {
        supervisorScope {
            var generation = 1
            while (true) {
                try {
                    val index = this.launchIndex(dir, cfg, generation++, statusFlow)
                    indexRef.set(index)
                    startedLatch.complete(Unit)
                    index.await()
                } catch (e: Throwable) {
                    if (e is CancellationException) {
                        this.ensureActive()
                    }
                    println("Index failed with $e, rebuilding index!")
                    if (cfg.enableLogging.get()) e.printStackTrace(System.out)
                    println()
                }
            }
        }
    }

    startedLatch.await()

    return object : Index, Deferred<Any?> by job {
        override suspend fun status(): StatusResult {
            return indexRef.get().status()
        }

        override suspend fun statusFlow(): Flow<StatusResult> {
            return statusFlow
        }

        override suspend fun find(query: String): Flow<SearchResult> {
            return indexRef.get().find(query)
        }

        override suspend fun enableLogging() {
            return indexRef.get().enableLogging()
        }

        override suspend fun disableLogging() {
            return indexRef.get().disableLogging()
        }
    }
}

@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.launchIndex(
    dir: Path,
    cfg: IndexConfig,
    generation: Int = 0,
    statusFlow: MutableStateFlow<StatusResult> = MutableStateFlow(StatusResult.broken()),
): Index {

    val userRequests = Channel<UserRequest>()
    val searchInFileRequests = Channel<SearchInFileRequest>()
    val indexUpdateRequests = Channel<IndexUpdateRequest>()
    val statusUpdates = Channel<StatusUpdate>(Int.MAX_VALUE)
    val fileEvents = Channel<FileEvent>(Int.MAX_VALUE)

    val job = async {
        coroutineScope {
            launch(CoroutineName("watcher")) { watcher(cfg, dir, fileEvents, statusUpdates) }
            repeat(4) {
                launch(CoroutineName("indexer-$it")) { indexer(cfg, fileEvents, indexUpdateRequests) }
            }
            launch(CoroutineName("index")) {
                index(cfg, generation, userRequests, indexUpdateRequests, statusUpdates, statusFlow)
            }
            repeat(4) {
                launch(CoroutineName("searchInFile-$it")) { searchInFile(cfg, searchInFileRequests) }
            }
        }
        null // wtf
    }

    job.invokeOnCompletion {
        userRequests.cancel(it?.let { CancellationException(it.message, it) })
        searchInFileRequests.cancel(it?.let { CancellationException(it.message, it) })
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

        override suspend fun find(query: String): Flow<SearchResult> {
            return withIndexContext {
                val result = CompletableDeferred<Flow<FileAddress>>()
                val request = FindRequest(
                    query = query,
                    result = result,
                )
                userRequests.send(request)
                val flow = result.await()
                flow
                    .buffer(Int.MAX_VALUE)
                    .map { fa ->
                        flow {
                            val searchResults = withIndexContext {
                                val flowFuture = CompletableDeferred<List<SearchResult>>()
                                searchInFileRequests.send(SearchInFileRequest(fa, query, flowFuture))
                                flowFuture.await()
                            } ?: listOf()
                            emitAll(searchResults.asFlow())
                        }
                    }
                    .flattenMerge(concurrency = 4) // bounded by searchInFile coroutine concurrency
            } ?: flowOf()
        }

        override suspend fun statusFlow(): Flow<StatusResult> {
            return statusFlow
        }

        override suspend fun enableLogging() {
            cfg.enableLogging.set(true)
        }

        override suspend fun disableLogging() {
            cfg.enableLogging.set(false)
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

