import indexer.core.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.selects.select
import java.lang.management.ManagementFactory
import kotlin.io.path.Path

suspend fun main() = try {
    withContext(Dispatchers.Default + CoroutineName("main")) {

        val stdin = startStdReaderDaemon()

//        val cfg = indexer.core.wordIndexConfig(enableWatcher = true)
        val cfg = indexer.core.trigramIndexConfig(enableWatcher = false)

        val dir = "."
//            val dir = "/Users/akapelyushok/git_tree/main"
//            val dir = "/Users/akapelyushok/Projects/intellij-community1"

        val index = launchIndex(Path(dir), cfg)
        val searchEngine = launchSearchEngine(cfg, index)
        launchStatusDisplay(searchEngine)

        runCmdHandler(stdin, searchEngine, index, cfg)

        cancel()
    }
} catch (e: CancellationException) {
    // ignore
}

private fun CoroutineScope.launchStatusDisplay(searchEngine: SearchEngine) {
    launch(CoroutineName("displayStatus")) {
        var wasInSync = false
        searchEngine.indexStatusUpdates().collect { update ->
            when (update) {
                is IndexStatusUpdate.Initial -> {
                    println("Index start!")
                }

                is IndexStatusUpdate.Initializing ->
                    println("Initializing index!")

                is IndexStatusUpdate.IndexInSync -> {
                    if (!wasInSync) {
                        wasInSync = true
                        println("Initial sync completed after ${update.ts - update.status.lastRestartTime}ms!")
                    } else {
                        println("Index in sync again!")
                    }
                }

                is IndexStatusUpdate.IndexOutOfSync -> {
                    println("Index out of sync!")
                }

                is IndexStatusUpdate.AllFilesDiscovered ->
                    println("All files discovered after ${update.ts - update.status.lastRestartTime}ms!")

                is IndexStatusUpdate.Failed -> {
                    println("Index failed with exception ${update.reason}")
                    update.reason.printStackTrace(System.out)
                }

                is IndexStatusUpdate.WatcherStarted ->
                    println("Watcher started after after ${update.ts - update.status.lastRestartTime}ms!")

                is IndexStatusUpdate.ReinitializingBecauseFileSyncFailed -> {
                    wasInSync = false
                    println("File sync failed with ${update.reason} - reinitializing it")
                    update.reason.printStackTrace(System.out)
                }
            }
            println()
        }
    }
}

fun startStdReaderDaemon(): ReceiveChannel<String> {
    val channel = Channel<String>()
    Thread {
        generateSequence { readlnOrNull() }.forEach {
            channel.trySendBlocking(it).getOrThrow()

        }
    }
        .apply { isDaemon = true }
        .start()
    return channel
}

private suspend fun runCmdHandler(
    input: ReceiveChannel<String>,
    searchEngine: SearchEngine,
    index: Index,
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
                val state = searchEngine.indexState()
                println(state)
                println()
                println(state.prettyPrint())
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

            prompt == "compact" -> {
                index.compact()
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

                    val initialState = searchEngine.indexState()
                    val isInSyncBeforeSearch = initialState.isInSync()
                    if (!isInSyncBeforeSearch) {
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

                    if (isInSyncBeforeSearch) {
                        val currentStatus = searchEngine.indexState()
                        if (initialState != currentStatus) {
                            println("Directory content has changed during search, results might be incomplete or outdated")
                            println()
                        }
                    }
                }

                select {
                    input.onReceive {
                        println("Search interrupted!")
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
