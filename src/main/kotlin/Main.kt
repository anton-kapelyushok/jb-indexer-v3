import indexer.core.*
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
        runBlocking(Dispatchers.Default + CoroutineName("main")) {
            val stdin = Channel<String>()
            launch { readStdin(stdin) }

            val cfg = indexer.core.wordIndexConfig(enableWatcher = true)
//        val cfg = indexer.core.trigramIndexConfig(enableWatcher = true)

//            val dir = "."
//        val dir = "/Users/akapelyushok/git_tree/main"
            val dir = "/Users/akapelyushok/Projects/intellij-community"

            val index = launchResurrectingIndex(this, Path(dir), cfg)
            val searchEngine = launchSearchEngine(this, index)

            launch(CoroutineName("displayStatus")) {
                var prevStatus = StatusResult.broken()
                index.statusFlow().collect { newStatus ->
                    if (prevStatus.isBroken && !newStatus.isBroken) {
                        println("Initializing index!")
                    }
                    if (!prevStatus.isBroken && newStatus.isBroken) {
                        println("Index broke with exception ${newStatus.exception}:(")
                        newStatus.exception?.printStackTrace(System.out)
                    }
                    if (prevStatus.initialSyncTime == null && newStatus.initialSyncTime != null) {
                        println("Initial sync completed after ${newStatus.initialSyncTime}ms!")
                    }
                    if (prevStatus.watcherStartTime == null && newStatus.watcherStartTime != null) {
                        println("Watcher initialized after ${newStatus.watcherStartTime}ms!")
                    }
                    println()
                    prevStatus = newStatus
                }
            }

            runCmdHandler(stdin, searchEngine, cfg)
            searchEngine.cancel()
        }
    } catch (e: CancellationException) {
        // ignore
    }
}

private suspend fun readStdin(output: SendChannel<String>) {
    withContext(Dispatchers.IO + CoroutineName("stdReader")) {
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
    searchEngine: SearchEngine,
    cfg: IndexConfig,
) = withContext(CoroutineName("cmdHandler")) {
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
                cfg.enableLogging.set(true)
            }

            prompt == "status" -> {
                println(searchEngine.indexStatus())
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
                cfg.enableLogging.set(false)
            }

            prompt == "error" -> {
                error("error")
            }

            prompt.startsWith("find ") -> coroutineScope {
                val query = prompt.substring("find ".length)
                val job = launch {

                    val initialStatus = searchEngine.indexStatus()
                    val showInitialWarning =
                        initialStatus.initialSyncTime == null
                                || initialStatus.handledFileModifications != initialStatus.totalFileModifications
                                || initialStatus.isBroken
                    if (showInitialWarning) {
                        println("Directory is not fully indexed yet, results might be incomplete or outdated")
                        println()
                    }

                    searchEngine
                        .find(query)
                        .take(20)
                        .collect { (path, lineNo, line) ->
                            println("$path:$lineNo")
                            println(if (line.length > 100) line.substring(0..100) + "..." else line)
                            println()
                        }

                    if (!showInitialWarning) {
                        val currentStatus = searchEngine.indexStatus()
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
