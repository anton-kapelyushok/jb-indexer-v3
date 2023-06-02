package indexer.core

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.selects.select
import java.io.File
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

val enableLogging = AtomicBoolean(false)

@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.launchIndex(dir: Path): Index {
    val indexRequests = Channel<IndexRequest>(onUndeliveredElement = {
        it.onMessageLoss()
    })

    val job = launch {
        val watchEvents = Channel<WatchEvent>(Int.MAX_VALUE)
        launch(CoroutineName("watcher")) { watcher(dir, watchEvents) }
        repeat(4) {
            launch(CoroutineName("indexer-$it")) { indexer(watchEvents, indexRequests) }
        }
        launch(CoroutineName("index")) {
            index(indexRequests)
        }
    }

    return object : Index, Job by job {
        override suspend fun status(): StatusResult {
            val future = CompletableDeferred<StatusResult>()
            indexRequests.send(StatusRequest(future))
            return future.await()
        }

        override suspend fun find(query: String): Flow<SearchResult> {
            val flow = callbackFlow<FileAddress> {
                val request = FindTokenRequest(
                    query = query,
                    isConsumerAlive = { this.coroutineContext.isActive },
                    onResult = {
                        try {
                            // From `channel.send` description:
                            //
                            // Closing a channel after this function has suspended does not cause this suspended
                            // send invocation to abort.
                            //
                            // We work around it by waiting for job cancellation additionally
                            select {
                                channel.onSend(it) {}
                                coroutineContext.job.onJoin {
                                    // channel is closed at this point, will throw close exception
                                    channel.send(it)
                                }
                            }
                            Result.success(Unit)
                        } catch (e: Throwable) {
                            // either producer was canceled or channel was closed/cancelled by consumer
                            ensureActive()
                            Result.failure(e)
                        }
                    },
                    onFinish = { close() },
                    onError = { e ->
                        println("Search failed with $e")
                        close()
                    }
                )

                indexRequests.send(request)

                awaitClose {}
            }
                .buffer(capacity = Channel.RENDEZVOUS)

            // TODO: search should be separate thingy
            return flow
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

