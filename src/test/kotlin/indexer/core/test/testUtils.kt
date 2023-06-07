package indexer.core.test

import indexer.core.internal.FileAddress
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.test.runTest
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds


fun fa(s: String) = FileAddress(s)


fun runTestWithFilesystem(timeout: Duration = 1.seconds, fn: suspend CoroutineScope.(dir: Path) -> Unit) {
    val dir = Files.createTempDirectory("tmp-dir")
    return try {
        runTest(timeout = timeout) {
            fn(dir)
        }
    } finally {
        Files.walk(dir)
            .map(Path::toFile)
            .forEach(File::delete)
    }
}

fun Path.child(name: String): Path {
    return Paths.get(this.toString(), name)
}

fun Path.toFileAddress() = FileAddress(toFile().canonicalPath)
fun FileAddress.toPath() = Paths.get(path)