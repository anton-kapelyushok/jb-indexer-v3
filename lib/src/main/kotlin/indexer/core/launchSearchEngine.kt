package indexer.core

import indexer.core.internal.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*

@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.launchSearchEngine(cfg: IndexConfig, index: Index): SearchEngine {
    val searchInFileRequests = Channel<SearchInFileRequest>()

    val deferred = async(Dispatchers.Default + CoroutineName("launchSearchEngine")) {
        index.join()
        when (val e = index.getCompletionExceptionOrNull()) {
            null -> throw IllegalStateException("Underlying index completed")
            is CancellationException -> throw IllegalStateException("Underlying index was canceled", e)
            else -> IllegalStateException("Underlying index failed with exception", e)
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
                cfg.find(index, query)
                    .distinct()
                    .flatMapMerge(concurrency = 4) { fileCandidate -> searchInFile(cfg, fileCandidate, query).asFlow() }
            } ?: flowOf()
        }

        override suspend fun cancelAll(cause: CancellationException?) {
            index.cancel(cause)
            cancel()
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
