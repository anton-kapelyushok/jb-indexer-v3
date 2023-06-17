package indexer.core

import indexer.core.internal.FileAddress
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

@OptIn(ExperimentalCoroutinesApi::class)
fun wordIndexConfig(
    handleWatcherError: (e: Throwable) -> Unit = {},
    handleInitialFileSyncError: suspend (e: Throwable) -> Unit = {},
    enableWatcher: Boolean = true,
) = object : IndexConfig {
    private val regex = Regex("""\W+""")

    override val enableLogging = AtomicBoolean(false)

    override val enableWatcher: Boolean = enableWatcher

    override val ioSemaphore: Semaphore = Semaphore(4)

    override fun tokenize(line: String): List<String> {
        return line.split(regex).map { it.trim().lowercase() }
            .filter { it.isNotEmpty() }
    }

    override suspend fun find(index: Index, query: String) = flow<FileAddress> {
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
                val startTokenIsFull = regex.matches("${query.first()}")
                val endTokenIsFull = regex.matches("${query.last()}")

                @Suppress("KotlinConstantConditions")
                val tokens = when {
                    startTokenIsFull && endTokenIsFull -> listOf(searchToken)
                    !startTokenIsFull && !endTokenIsFull -> index.findTokensMatchingPredicate {
                        it.contains(searchToken)
                    }

                    !startTokenIsFull -> index.findTokensMatchingPredicate { it.endsWith(searchToken) }
                    !endTokenIsFull -> index.findTokensMatchingPredicate { it.startsWith(searchToken) }
                    else -> error("unreachable")
                }

                tokens
                    .asFlow()
                    .flatMapConcat { index.findFilesByToken(it).asFlow() }
                    .collect { emit(it) }
            }

            2 -> {
                val (startToken, endToken) = searchTokens

                val startTokenIsFull = regex.matches("${query.first()}")
                val endTokenIsFull = regex.matches("${query.last()}")

                val startFullMatch = index.findFilesByToken(startToken).toSet()
                val endFullMatch = index.findFilesByToken(endToken).toSet()

                startFullMatch.intersect(endFullMatch).forEach { emit(it) }

                val endStartsWith =
                    if (!endTokenIsFull)
                        index.findTokensMatchingPredicate { it.startsWith(endToken) }.toSet()
                    else setOf()

                val endStartsWithFiles = endStartsWith.asFlow()
                    .flatMapConcat { index.findFilesByToken(it).asFlow() }
                    .onEach { if (it in startFullMatch) emit(it) }
                    .toSet()

                val startEndsWith = if (!startTokenIsFull)
                    index.findTokensMatchingPredicate { it.endsWith(startToken) }.toSet()
                else
                    setOf()

                val startEndsWithFiles = startEndsWith.asFlow()
                    .flatMapConcat { index.findFilesByToken(it).asFlow() }
                    .onEach { if (it in endFullMatch) emit(it) }
                    .toSet()

                startEndsWithFiles.intersect(endStartsWithFiles).forEach { emit(it) }
            }

            else -> {
                val startTokenIsFull = regex.matches("${query.first()}")
                val endTokenIsFull = regex.matches("${query.last()}")

                val coreTokens = searchTokens.subList(1, searchTokens.lastIndex)

                var fileSet = index.findFilesByToken(coreTokens[0]).toSet()
                for (i in 1 until coreTokens.size) {
                    if (fileSet.isEmpty()) break
                    val newFiles = index.findFilesByToken(coreTokens[i]).toSet()
                    fileSet = fileSet.intersect(newFiles)
                }
                if (fileSet.isEmpty()) return@flow

                val startToken = searchTokens.first()
                val endToken = searchTokens.last()

                val startTokenFullMatch = index.findFilesByToken(startToken).toSet()
                val endTokenFullMatch = index.findFilesByToken(endToken).toSet()

                fileSet.intersect(startTokenFullMatch).intersect(endTokenFullMatch).forEach { emit(it) }

                if (startTokenIsFull && endTokenIsFull) return@flow

                fileSet = if (!startTokenIsFull) {
                    val tokensInIndex = index.findTokensMatchingPredicate { it.endsWith(startToken) }.toSet()
                    val newFiles = tokensInIndex.flatMap { index.findFilesByToken(it) }.toSet()
                    fileSet.intersect(newFiles).also {
                        it.intersect(endTokenFullMatch).forEach { f -> emit(f) }
                    }
                } else {
                    fileSet.intersect(startTokenFullMatch)
                }

                if (fileSet.isEmpty()) return@flow

                fileSet = if (!endTokenIsFull) {
                    val tokensInIndex = index.findTokensMatchingPredicate { it.startsWith(endToken) }.toSet()
                    val newFiles = tokensInIndex.flatMap { index.findFilesByToken(it) }.toSet()
                    fileSet.intersect(newFiles).also {
                        it.intersect(startTokenFullMatch).forEach { f -> emit(f) }
                    }
                } else {
                    fileSet.intersect(endTokenFullMatch)
                }

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