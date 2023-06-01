import indexer.core.assembled
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import java.nio.file.Path
import java.util.concurrent.Executors
import kotlin.io.path.Path

fun main() {
    runBlocking {
        val stdin = Channel<String>()
        val stdinReader = launch { readStdin(stdin) }
        runIndex(stdin, Path("."))
        stdinReader.cancel()
    }
}

suspend fun readStdin(output: SendChannel<String>) {
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

suspend fun runIndex(input: ReceiveChannel<String>, dir: Path) {
    while (true) {
        try {
            assembled(input, dir)
            break
        } catch (e: Throwable) {
            if (e is CancellationException) throw e
            println("Indexer failed with $e")
            e.printStackTrace(System.out)
            println("Restarting!")
        }
    }
}