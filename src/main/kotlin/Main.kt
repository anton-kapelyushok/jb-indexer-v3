import indexer.core.Index
import indexer.core.enableLogging
import indexer.core.launchIndex
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.selects.select
import java.lang.management.ManagementFactory
import java.nio.file.Path
import java.util.concurrent.Executors
import kotlin.io.path.Path


fun main() {
    runBlocking {
        val stdin = Channel<String>()
        val stdinReader = launch { readStdin(stdin) }
//        runIndex(stdin, Path("."))
//        runIndex(stdin, Path("/Users/akapelyushok/git_tree/main"))
        runIndex(stdin, Path("/Users/akapelyushok/Projects/intellij-community"))
        stdinReader.cancel()
    }
}

private suspend fun readStdin(output: SendChannel<String>) {
    withContext(Dispatchers.IO) {
        val stdinReaderExecutor =
            Executors.newSingleThreadExecutor {
                Executors.defaultThreadFactory().newThread(it).apply { isDaemon = true }
            }

        val future = stdinReaderExecutor.submit {
            generateSequence { readlnOrNull() }.forEach {
                runBlocking(coroutineContext) {
                    output.send(it)
                }
            }
        }

        runInterruptible {
            future.get()
        }
    }
}

private suspend fun runIndex(input: ReceiveChannel<String>, dir: Path) {
    while (true) {
        try {
            withContext(Dispatchers.Default) {
                val index = launchIndex(dir)
                runCmdHandler(input, index)
                index.cancel()
            }
            break
        } catch (e: Throwable) {
            if (e is CancellationException) throw e
            println("Indexer failed with $e")
            e.printStackTrace(System.out)
            println("Restarting!")
        }
    }
}

private suspend fun runCmdHandler(
    input: ReceiveChannel<String>,
    index: Index,
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
                println(index.status())
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

            prompt.startsWith("/find ") -> coroutineScope {
                enableLogging.set(true)
                val query = prompt.substring("/find ".length)
                val job = launch {
                    index
                        .find(query)
                        .take(20)
                        .collect { (path, lineNo, line) ->
                            delay(100)
                            println("$path:$lineNo")
                            println(if (line.length > 100) line.substring(0..100) + "..." else line)
                            println()
                        }
                }

                select {
                    input.onReceive {
                        job.cancel(CancellationException("stop please :( $it"))
                    }
                    job.onJoin {}
                }
                delay(30)
                enableLogging.set(false)
            }

            else -> {
                println("Unrecognized command!")
            }
        }

        println("====== ${System.currentTimeMillis() - start}ms")
        println()
    }
}
