package indexer.core

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.lang.management.ManagementFactory
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

val enableLogging = AtomicBoolean(false)

suspend fun assembled(dir: Path) = coroutineScope {
    val outer = this
    val indexRequests = Channel<IndexRequest>()
    val watchEvents = Channel<WatchEvent>()

    launch { watcher(dir, watchEvents) }
    (1..4).forEach {
        launch { indexer(watchEvents, indexRequests) }
    }
    launch {
        index(indexRequests)
    }

    println("/stop to stop")
    println()

    launch(Dispatchers.IO) {
        withCancellationCallback({ System.`in`.close() }) {
            while (true) {
                val prompt = readln()

                val start = System.currentTimeMillis()
                when {
                    prompt == "/stop" -> {
                        outer.cancel()
                        return@withCancellationCallback
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
    }
}


