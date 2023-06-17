package indexer.core.internal

import indexer.core.IndexConfig
import indexer.core.IndexSearchResult
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import java.io.File
import java.io.IOException

internal suspend fun searchInFile(
    cfg: IndexConfig,
    fa: FileAddress,
    query: String,
): List<IndexSearchResult> = cfg.ioSemaphore.withPermit {
    withContext(Dispatchers.IO) {
        val results = try {
            File(fa.path)
                .readLines()
                .withIndex()
                .filter { (_, line) -> cfg.matches(line, query) }
                .map { (idx, line) -> IndexSearchResult(fa.path, idx + 1, line) }
        } catch (e: IOException) {
            cfg.debugLog("Failed to read ${fa.path}: $e")
            listOf()
        }
        results
    }
}