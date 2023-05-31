import indexer.core.assembled
import indexer.core.withCancellationCallback
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import java.nio.file.Path
import kotlin.io.path.Path

fun main(args: Array<String>) {
    runBlocking {
//        assembled(Path("/Users/akapelyushok/Projects/intellij-community"))

        val stdin = Channel<String>()
        launch(Dispatchers.IO) {
            withCancellationCallback({
                withContext(Dispatchers.IO) {
                    System.`in`.close()
                }
            }) {
                generateSequence { readlnOrNull() }.forEach { stdin.send(it) }
            }
        }

        runIndex(stdin, Path("."))
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