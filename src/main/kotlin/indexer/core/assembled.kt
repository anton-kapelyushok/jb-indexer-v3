package indexer.core

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.*
import java.io.File
import java.lang.management.ManagementFactory
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

val enableLogging = AtomicBoolean(false)

suspend fun assembled(input: ReceiveChannel<String>, dir: Path) = withContext(Dispatchers.Default) {
    val indexRequests = Channel<IndexRequest>(onUndeliveredElement = {
        it.onMessageLoss()
    })

    val indexJob = launch {
        val watchEvents = Channel<WatchEvent>()
        launch(CoroutineName("watcher")) { watcher(dir, watchEvents) }
        repeat(4) {
            launch(CoroutineName("indexer-$it")) { indexer(watchEvents, indexRequests) }
        }
        launch(CoroutineName("index")) {
            index(indexRequests)
        }
    }

    rmdCmdHandler(input, indexRequests)
    indexJob.cancel()
}

@OptIn(ExperimentalCoroutinesApi::class)
private suspend fun rmdCmdHandler(
    input: ReceiveChannel<String>,
    indexRequests: Channel<IndexRequest>
) {
    val helpMessage = "Available commands: /find /find2 /stop /enable-logging /status /gc /memory /error /help"
    println(helpMessage)
    println()

    for (prompt in input) {
        val start = System.currentTimeMillis()
        when {
            prompt == "/stop" -> {
                break
            }

            prompt == "/help" -> {
                println(helpMessage)
            }

            prompt == "/enable-logging" -> {
                enableLogging.set(true)
            }

            prompt == "/status" -> {
                val future = CompletableDeferred<StatusResult>()
                indexRequests.send(StatusRequest(future))
                val result = future.await()
                println(result)
            }

            prompt == "/gc" -> {
                val memoryBefore = "${ManagementFactory.getMemoryMXBean().heapMemoryUsage.used / 1_000_000} MB"
                System.gc()
                val memoryAfter = "${ManagementFactory.getMemoryMXBean().heapMemoryUsage.used / 1_000_000} MB"
                println("$memoryBefore -> $memoryAfter")
            }

            prompt == "/memory" -> {
                println("${ManagementFactory.getMemoryMXBean().heapMemoryUsage.used / 1_000_000} MB")
            }

            prompt == "" -> {
                enableLogging.set(false)
            }

            prompt == "/error" -> {
                error("/error")
            }

            prompt.startsWith("/find ") -> {
                val query = prompt.substring("/find".length + 1)
                val future = CompletableDeferred<List<FileAddress>>()
                indexRequests.send(FindTokenRequest(query, future))
                future
                    .await()
                    .take(5)
                    .forEach {
                        println(it.path)
                    }
            }

            prompt.startsWith("/find2 ") -> {
                val query = prompt.substring("/find2".length + 1)
                val flow = callbackFlow<FileAddress> {
                    val request = FindTokenRequest2(
                        query = query,
                        isConsumerAlive = { this.coroutineContext.isActive },
                        onResult = {
                            coroutineScope {
                                try {
                                    channel.send(it)
                                } catch (e: Throwable) {
                                    // onResult callback is executed in Producer context
                                    // CancellationException can be thrown
                                    // 1. when consumer is closed (for example `AbortFlowException: Flow was aborted, no more elements needed`)
                                    //    in that case we can swallow the exception
                                    // 2. when producer is closed - rethrow the exception
                                    if (e is CancellationException && isActive) {
                                        return@coroutineScope
                                    }
                                    throw e
                                }
                            }

                        },
                        onFinish = { close() },
                        onError = { e ->
                            println("Search failed with exception $e")
                            close()
                        }
                    )

                    indexRequests.send(request)

                    awaitClose {}
                }
                    .buffer(capacity = Channel.RENDEZVOUS)

                data class SearchResult(val path: String, val lineNo: Int, val line: String)

                flow
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
                    .take(5)
                    .collect { (path, lineNo, line) ->
                        println("$path:$lineNo")
                        println(if (line.length > 100) line.substring(0..100) + "..." else line)
                        println()
                    }
            }

            else -> {
                println("Unrecognized command!")
            }
        }

        println("====== ${System.currentTimeMillis() - start}ms")
        println()
    }
}


