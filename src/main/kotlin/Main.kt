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
            val stdinReader = launch { readStdin(stdin) }

            val cfg = indexer.core.wordIndexConfig(enableWatcher = true)
//        val cfg = indexer.core.trigramIndexConfig(enableWatcher = true)

            val dir = "."
//            val dir = "/Users/akapelyushok/git_tree/main"
//            val dir = "/Users/akapelyushok/Projects/intellij-community"

            val index = launchIndex(Path(dir), cfg)
            val searchEngine = launchSearchEngine(cfg, index)
            launchStatusDisplay(searchEngine)

            runCmdHandler(stdin, searchEngine, cfg)

            cancel()
        }
    } catch (e: CancellationException) {
        // ignore
    }
}

private fun CoroutineScope.launchStatusDisplay(searchEngine: SearchEngine) {
    launch(CoroutineName("displayStatus")) {
        var wasInSync = false
        searchEngine.indexStatusUpdates().collect { update ->
            when (update) {
                is IndexStateUpdate.Initial -> {
                    println("Index start!")
                }

                is IndexStateUpdate.Initializing ->
                    println("Initializing index!")

                is IndexStateUpdate.IndexInSync -> {
                    if (!wasInSync) {
                        wasInSync = true
                        println("Initial sync completed after ${update.ts - update.status.lastRestartTime}ms!")
                    } else {
                        println("Index in sync again!")
                    }
                }

                is IndexStateUpdate.IndexOutOfSync -> {
                    println("Index out of sync!")
                }

                is IndexStateUpdate.AllFilesDiscovered ->
                    println("All files discovered after ${update.ts - update.status.lastRestartTime}ms!")

                is IndexStateUpdate.Failed -> {
                    println("Index failed with exception ${update.reason}")
                    update.reason.printStackTrace(System.out)
                }

                is IndexStateUpdate.WatcherStarted ->
                    println("Watcher started after after ${update.ts - update.status.lastRestartTime}ms!")

                is IndexStateUpdate.ReinitializingBecauseWatcherFailed -> {
                    wasInSync = false
                    println("Watcher failed with ${update.reason} - reinitializing it")
                    update.reason.printStackTrace(System.out)
                }
            }
            println()
        }
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
                        !initialStatus.allFileDiscovered
                                || initialStatus.handledFileEvents != initialStatus.totalFileEvents
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
                        if (initialStatus != currentStatus) {
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
