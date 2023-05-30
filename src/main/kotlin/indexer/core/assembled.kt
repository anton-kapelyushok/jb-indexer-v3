package indexer.core

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
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
        while (true) {
            val prompt = readln()
            if (prompt == "/stop") {
                outer.cancel()
                return@launch
            }
            if (prompt == "/enable-logging") {
                enableLogging.set(true)
                continue
            }
            if (prompt == "/status") {
                val future = CompletableDeferred<StatusResult>()
                indexRequests.send(StatusRequest(future))
                val result = future.await()
                println(result)
            }

            if (prompt == "") {
                enableLogging.set(false)
                continue
            }

            if (prompt.startsWith("/find ")) {
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

            println("======")
            println()
        }
    }
}