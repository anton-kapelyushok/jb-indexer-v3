package indexer.core

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*

@OptIn(ExperimentalCoroutinesApi::class)
suspend fun launchSearchEngine(scope: CoroutineScope, index: Index): SearchEngine {
    val searchInFileRequests = Channel<SearchInFileRequest>()
    val cfg = index.config()

    val job = scope.async {
        repeat(4) {
            launch(CoroutineName("searchInFile-$it")) { searchInFile(cfg, searchInFileRequests) }
        }
        index.join()
        when (val e = index.getCompletionExceptionOrNull()) {
            null -> throw IllegalStateException("Underlying index completed")
            is CancellationException -> throw IllegalStateException("Underlying index was canceled", e)
            else -> throw java.lang.IllegalStateException("Underlying index failed with exception", e)
        }
    }

    job.invokeOnCompletion {
        searchInFileRequests.cancel(it?.let { CancellationException(it.message, it) })
    }

    return object : SearchEngine, Deferred<Any?> by job {
        override suspend fun indexStatus(): StatusResult {
            return index.status()
        }

        override suspend fun indexStatusFlow(): Flow<StatusResult> {
            return index.statusFlow()
        }

        override suspend fun find(query: String): Flow<SearchResult> {
            return index.findFileCandidates(query)
                .buffer(Int.MAX_VALUE)
                .flatMapMerge(concurrency = 4) { fileCandidate -> searchInFile(fileCandidate, query) }
        }

        override suspend fun enableLogging() {
            cfg.enableLogging.set(true)
        }

        override suspend fun disableLogging() {
            cfg.enableLogging.set(false)
        }

        private suspend fun searchInFile(fileCandidate: FileAddress, query: String): Flow<SearchResult> = flow {
            val searchResults = withSearchEngineContext {
                val future = CompletableDeferred<List<SearchResult>>()
                searchInFileRequests.send(SearchInFileRequest(fileCandidate, query, future))
                future.await()
            } ?: listOf()

            searchResults.forEach { emit(it) }
        }

        private suspend fun <T : Any> withSearchEngineContext(
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
