import indexer.core.assembled
import kotlinx.coroutines.runBlocking
import kotlin.io.path.Path

fun main(args: Array<String>) {
    runBlocking {
//        assembled(Path("/Users/akapelyushok/Projects/intellij-community"))
        assembled(Path("."))
    }
}