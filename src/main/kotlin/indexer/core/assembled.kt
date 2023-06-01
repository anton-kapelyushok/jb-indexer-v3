package indexer.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import java.lang.management.ManagementFactory
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

val enableLogging = AtomicBoolean(false)

suspend fun assembled(input: ReceiveChannel<String>, dir: Path) = coroutineScope {
    val indexRequests = Channel<IndexRequest>()

    val indexJob = launch {
        val watchEvents = Channel<WatchEvent>()
        launch { watcher(dir, watchEvents) }
        repeat(4) {
            launch { indexer(watchEvents, indexRequests) }
        }
        launch {
            index(indexRequests)
        }
    }

    rmdCmdHandler(input, indexRequests)
    indexJob.cancel()
}

private suspend fun rmdCmdHandler(
    input: ReceiveChannel<String>,
    indexRequests: Channel<IndexRequest>
) {
    val helpMessage = "Available commands: /find /stop /enable-logging /status /gc /memory /error /help"
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

            else -> {
                println("Unrecognized command!")
            }
        }

        println("====== ${System.currentTimeMillis() - start}ms")
        println()
    }
}


