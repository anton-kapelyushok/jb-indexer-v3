import indexer.core.Index
import indexer.core.launchResurrectingIndex
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.selects.select
import java.lang.management.ManagementFactory
import java.util.concurrent.Executors
import kotlin.io.path.Path

fun main() {
    try {
        runBlocking(Dispatchers.Default) {
            val stdin = Channel<String>()
            launch { readStdin(stdin) }

            val cfg = indexer.core.wordIndexConfig(enableWatcher = true)
//        val cfg = indexer.core.trigramIndexConfig(enableWatcher = true)

            val dir = "."
//        val dir = "/Users/akapelyushok/git_tree/main"
//        val dir = "/Users/akapelyushok/Projects/intellij-community"

            val index = launchResurrectingIndex(this, Path(dir), cfg)

            launch {
                index.statusFlow().collect {
                    if (cfg.enableLogging.get()) println("Status update: $it")
                }
            }

            runCmdHandler(stdin, index)
            cancel()
        }
    } catch (e: CancellationException) {
        // ignore
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

private suspend fun runCmdHandler(
    input: ReceiveChannel<String>,
    index: Index,
) {
    val helpMessage = "Available commands: find stop enable-logging status gc memory error help"
    println(helpMessage)
    println()

    for (prompt in input) {
        val start = System.currentTimeMillis()
        when {
            prompt == "stop" -> {
                break
            }

            prompt == "help" -> {
                println(helpMessage)
            }

            prompt == "enable-logging" -> {
                index.enableLogging()
            }

            prompt == "status" -> {
                println(index.status())
            }

            prompt == "gc" -> {
                val memoryBefore = "${ManagementFactory.getMemoryMXBean().heapMemoryUsage.used / 1_000_000} MB"
                System.gc()
                val memoryAfter = "${ManagementFactory.getMemoryMXBean().heapMemoryUsage.used / 1_000_000} MB"
                println("$memoryBefore -> $memoryAfter")
            }

            prompt == "memory" -> {
                println("${ManagementFactory.getMemoryMXBean().heapMemoryUsage.used / 1_000_000} MB")
            }

            prompt == "" -> {
                index.disableLogging()
            }

            prompt == "error" -> {
                error("error")
            }

            prompt.startsWith("find ") -> coroutineScope {
                val query = prompt.substring("find ".length)
                val job = launch {

                    val initialStatus = index.status()
                    val showInitialWarning =
                        initialStatus.initialSyncTime == null
                                || initialStatus.handledFileModifications != initialStatus.totalFileModifications
                                || initialStatus.isBroken
                    if (showInitialWarning) {
                        println("Directory is not fully indexed yet, results might be incomplete or outdated")
                        println()
                    }

                    index
                        .find(query)
                        .take(20)
                        .collect { (path, lineNo, line) ->
                            println("$path:$lineNo")
                            println(if (line.length > 100) line.substring(0..100) + "..." else line)
                            println()
                        }

                    if (!showInitialWarning) {
                        val currentStatus = index.status()
                        if (currentStatus.totalFileModifications != initialStatus.totalFileModifications
                            || currentStatus.generation != initialStatus.generation
                            || currentStatus.isBroken
                        ) {
                            println("Directory content has changed during search, results might be incomplete or outdated")
                            println()
                        }
                    }
                }

                select {
                    input.onReceive {
                        job.cancel(CancellationException("stop please :( $it"))
                    }
                    job.onJoin {}
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
