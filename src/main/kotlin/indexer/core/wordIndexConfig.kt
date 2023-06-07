package indexer.core

import indexer.core.internal.FileAddress
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import java.util.concurrent.atomic.AtomicBoolean

@OptIn(ExperimentalCoroutinesApi::class)
fun wordIndexConfig(
    enableWatcher: Boolean = true,
    handleWatcherError: (e: Throwable) -> Unit = {},
    handleInitialFileSyncError: suspend (e: Throwable) -> Unit = {},
) = object : IndexConfig {
    private val regex = Regex("""\W+""")

    override val enableLogging = AtomicBoolean(false)

    override val enableWatcher: Boolean = enableWatcher

    override fun tokenize(line: String): List<String> {
        return line.split(regex).map { it.trim().lowercase() }
            .filter { it.isNotEmpty() }
    }

    override suspend fun find(query: String, index: Index) = flow<FileAddress> {
        val searchTokens = tokenize(query).toList()
        when (searchTokens.size) {
            0 -> {
                // index won't help us here, emit everything we have
                val tokens = index.findTokensMatchingPredicate { true }
                tokens
                    .asFlow()
                    .flatMapConcat { index.findFilesByToken(it).asFlow() }
                    .collect { emit(it) }

                return@flow
            }

            1 -> {
                val searchToken = searchTokens[0].lowercase()
                index.findFilesByToken(searchToken).forEach { emit(it) }

                index.findTokensMatchingPredicate { it.contains(searchToken) }
                    .asFlow()
                    .flatMapConcat { index.findFilesByToken(it).asFlow() }
                    .collect { emit(it) }
            }

            2 -> {
                val (startToken, endToken) = searchTokens
                val startFullMatch = index.findFilesByToken(startToken).toSet()
                val endFullMatch = index.findFilesByToken(startToken).toSet()

                startFullMatch.intersect(endFullMatch).forEach { emit(it) }

                val endStartsWith = index.findTokensMatchingPredicate { it.startsWith(endToken) }.toSet()
                val endStartsWithFiles = endStartsWith.asFlow()
                    .flatMapConcat { index.findFilesByToken(it).asFlow() }
                    .onEach { if (it in startFullMatch) emit(it) }
                    .toSet()


                val startEndsWith = index.findTokensMatchingPredicate { it.endsWith(startToken) }.toSet()
                val startEndsWithFiles = startEndsWith.asFlow()
                    .flatMapConcat { index.findFilesByToken(it).asFlow() }
                    .onEach { if (it in endFullMatch) emit(it) }
                    .toSet()

                startEndsWithFiles.intersect(endStartsWithFiles).forEach { emit(it) }
            }

            else -> {
                val coreTokens = searchTokens.subList(1, searchTokens.lastIndex)
                var fileSet = index.findFilesByToken(coreTokens[0]).toSet()
                for (i in 1 until coreTokens.size) {
                    if (fileSet.isEmpty()) break
                    val newFiles = index.findFilesByToken(coreTokens[i]).toSet()
                    fileSet = fileSet.intersect(newFiles)
                }
                if (fileSet.isEmpty()) return@flow

                val startToken = searchTokens.first()
                val startFiles = index.findFilesByToken(startToken).toSet()

                val endToken = searchTokens.last()
                val endFiles = index.findFilesByToken(endToken).toSet()

                fileSet.intersect(startFiles).intersect(endFiles).forEach { emit(it) }
                fileSet.intersect(startFiles).forEach { emit(it) }
                fileSet.intersect(endFiles).forEach { emit(it) }
                fileSet.forEach { emit(it) }
            }
        }
    }

    override fun matches(line: String, query: String): Boolean {
        return line.contains(query)
    }

    override suspend fun handleWatcherError(e: Throwable) {
        handleWatcherError(e)
    }

    override suspend fun handleInitialFileSyncError(e: Throwable) {
        handleInitialFileSyncError(e)
    }
}