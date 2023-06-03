package indexer.core

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.nio.file.Path

@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.launchIndex(dir: Path, cfg: IndexConfig): Index {

    val userRequests = Channel<UserRequest>()
    val searchInFileRequests = Channel<SearchInFileRequest>()
    val indexRequests = Channel<IndexRequest>()
    val statusUpdates = Channel<StatusUpdate>(Int.MAX_VALUE)
    val fileEvents = Channel<FileEvent>(Int.MAX_VALUE)

    val job = launch {
        launch(CoroutineName("watcher")) { watcher(cfg, dir, fileEvents, statusUpdates) }
        repeat(4) {
            launch(CoroutineName("indexer-$it")) { indexer(cfg, fileEvents, indexRequests) }
        }
        launch(CoroutineName("index")) {
            index(cfg, userRequests, indexRequests, statusUpdates)
        }
        repeat(4) {
            launch(CoroutineName("searchInFile-$it")) { searchInFile(cfg, searchInFileRequests) }
        }
    }

    return object : Index, Job by job {
        override suspend fun status(): StatusResult {
            return withIndexContext {
                val future = CompletableDeferred<StatusResult>()
                userRequests.send(StatusRequest(future))
                future.await()
            }
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
                        val flowFuture = CompletableDeferred<Flow<SearchResult>>()
                        searchInFileRequests.send(SearchInFileRequest(fa, query, flowFuture))
                        flowFuture.await()
                    }
                    .flattenMerge(concurrency = 4) // bounded by searchInFile coroutine concurrency
            }
        }

        override suspend fun enableLogging() {
            cfg.enableLogging.set(true)
        }

        override suspend fun disableLogging() {
            cfg.enableLogging.set(false)
        }

        // future.await() may get stuck if index gets canceled while message is inflight
        private suspend fun <T> withIndexContext(
            block: suspend CoroutineScope.() -> T
        ): T {
            return try {
                withContext(job) {
                    block()
                }
            } catch (e: CancellationException) {
                ensureActive()
                throw RuntimeException("Index was cancelled during your operation :(", e)
            }
        }
    }
}

