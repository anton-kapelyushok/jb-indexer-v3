package indexer.core

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.io.File
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

val enableLogging = AtomicBoolean(false)

@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.launchIndex(dir: Path): Index {

    val userRequests = Channel<UserRequest>()

    val job = launch {
        val indexRequests = Channel<IndexRequest>()
        val statusUpdates = Channel<StatusUpdate>(Int.MAX_VALUE)
        val fileEvents = Channel<FileEvent>(Int.MAX_VALUE)
        launch(CoroutineName("watcher")) { watcher(dir, fileEvents, statusUpdates) }
        repeat(2) {
            launch(CoroutineName("indexer-$it")) { indexer(fileEvents, indexRequests) }
        }
        launch(CoroutineName("index")) {
            index(userRequests, indexRequests, statusUpdates)
        }
    }

    return object : Index, Job by job {
        override suspend fun status(): StatusResult {
            val future = CompletableDeferred<StatusResult>()
            userRequests.send(StatusRequest(future))
            return future.await()
        }

        override suspend fun find(query: String): Flow<SearchResult> {
            val result = CompletableDeferred<Flow<FileAddress>>()
            val request = FindRequest(
                query = query,
                result = result,
            )
            userRequests.send(request)
            val flow = result.await()
            // TODO: search should be separate thingy
            return flow
                .buffer(Int.MAX_VALUE)
                .flatMapConcat { fa ->
                    withContext(Dispatchers.IO) {
                        File(fa.path)
                            .readLines()
                            .withIndex()
                            .filter { (_, line) -> line.contains(query) }
                            .map { (idx, line) -> SearchResult(fa.path, idx + 1, line) }
                            .asFlow()
                    }
                }
        }
    }
}

