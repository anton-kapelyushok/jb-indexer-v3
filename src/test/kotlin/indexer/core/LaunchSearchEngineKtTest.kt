package indexer.core

import assertk.assertThat
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotEmpty
import indexer.core.test.child
import indexer.core.test.runTestWithFilesystem
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.toSet
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.deleteExisting
import kotlin.io.path.isRegularFile
import kotlin.io.path.readLines
import kotlin.io.path.writeText
import kotlin.time.Duration.Companion.seconds

class LaunchSearchEngineKtTest {
    @Test
    fun e2e() = runTestWithFilesystem(10.seconds) { tmpDir ->
        val dirToWatch = tmpDir.child("dirToWatch")

        FileUtils.copyDirectory(Paths.get("./src/test/resources/testdata").toFile(), dirToWatch.toFile())
        val config = trigramIndexConfig(enableWatcher = true)
        val index = launchIndex(dirToWatch, config)
        val searchEngine = launchSearchEngine(config, index)
        searchEngine.indexStatusUpdates().first { it is IndexStatusUpdate.IndexInSync }

        suspend fun checkQuery(query: String) = checkQuery(dirToWatch, searchEngine, query)

        checkQuery("poupa")
        checkQuery("path")
        checkQuery("date = { statu")
        checkQuery("async(CoroutineName")

        checkQuery("Каждому гарантируется свобода мысли и слова").also { assertThat(it).isNotEmpty() }
        searchEngine.catchUpdate {
            withContext(Dispatchers.IO) {
                dirToWatch.child("статья 29.txt").deleteExisting()
            }
        }
        checkQuery("Каждому гарантируется свобода мысли и слова").also { assertThat(it).isEmpty() }

        checkQuery("Граждане Российской Федерации имеют право собираться мирно без оружия").also { assertThat(it).isNotEmpty() }
        searchEngine.catchUpdate {
            withContext(Dispatchers.IO) {
                dirToWatch.child("статья 31.txt").writeText(
                    """
                    Колобок повесился
                """.trimIndent()
                )
            }
        }

        checkQuery("Граждане Российской Федерации имеют право собираться мирно без оружия").also { assertThat(it).isEmpty() }
        checkQuery("олобо").also { assertThat(it).isNotEmpty() }

        searchEngine.cancelAll()
    }

    private suspend fun SearchEngine.catchUpdate(fn: suspend () -> Unit) = coroutineScope {
        val se = this@catchUpdate
        val ready = CompletableDeferred<Unit>()
        launch {
            val currentState = se.indexState()
            val startingStatus = se.indexStatusUpdates()
                .first { it is IndexStatusUpdate.IndexInSync && it.status.clock == currentState.clock }
            assertThat(startingStatus).isInstanceOf(IndexStatusUpdate.IndexInSync::class)
            ready.complete(Unit)
            var metStartingStatus = false
            se.indexStatusUpdates()
                .onEach { metStartingStatus = metStartingStatus || it === startingStatus }
                .first { metStartingStatus && it is IndexStatusUpdate.IndexInSync && it != startingStatus }
        }
        ready.await()
        fn()
    }

    private suspend fun checkQuery(dir: Path, searchEngine: SearchEngine, query: String): Set<IndexSearchResult> {
        val expected = dumbSearch(dir, query)
        val actual = searchEngine.find(query).toSet()
        assertThat(actual, query).isEqualTo(expected)
        return actual
    }

    private fun dumbSearch(dir: Path, query: String): Set<IndexSearchResult> {
        val result = mutableSetOf<IndexSearchResult>()
        Files.walk(dir)
            .filter { it.isRegularFile() }
            .forEach { path ->
                path.readLines()
                    .withIndex()
                    .filter { (_, line) -> line.contains(query) }
                    .map { (idx, line) ->
                        IndexSearchResult(
                            path.toFile().canonicalPath,
                            idx + 1,
                            line
                        )
                    }
                    .forEach { result += (it) }
            }
        return result
    }
}