package indexer.core

import indexer.core.internal.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*

@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.launchSearchEngine(cfg: IndexConfig, index: Index): SearchEngine {
    val searchInFileRequests = Channel<SearchInFileRequest>()

    val deferred = async(CoroutineName("launchSearchEngine")) {
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

    return object : SearchEngine, Deferred<Any?> by deferred {
        override suspend fun indexState(): IndexState {
            return index.state()
        }

        override suspend fun indexStatusUpdates(): Flow<IndexStatusUpdate> {
            return index.statusFlow()
        }

        override suspend fun find(query: String): Flow<IndexSearchResult> {
            return withSearchEngineContext {
                flow<FileAddress> {
                    val tokens = cfg.tokenize(query)
                    if (tokens.isEmpty()) return@flow
                    var fileSet = index.findFilesByToken(tokens[0]).toSet()
                    for (i in 1 until tokens.size) {
                        if (fileSet.isEmpty()) break
                        val newFiles = index.findFilesByToken(tokens[i]).toSet()
                        fileSet = fileSet.intersect(newFiles)
                    }
                    for (f in fileSet) emit(f)
                }
                    .buffer(Int.MAX_VALUE)
                    .flatMapMerge(concurrency = 4) { fileCandidate -> searchInFile(fileCandidate, query) }
            } ?: flowOf()
        }

        override suspend fun cancelAll(cause: CancellationException?) {
            index.cancel(cause)
            cancel()
        }

        private suspend fun searchInFile(fileCandidate: FileAddress, query: String): Flow<IndexSearchResult> = flow {
            val searchResults = withSearchEngineContext {
                val future = CompletableDeferred<List<IndexSearchResult>>()
                searchInFileRequests.send(SearchInFileRequest(fileCandidate, query, future))
                future.await()
            } ?: listOf()

            searchResults.forEach { emit(it) }
        }

        private suspend fun <T : Any> withSearchEngineContext(
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
