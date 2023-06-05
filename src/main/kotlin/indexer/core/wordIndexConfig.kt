package indexer.core

import indexer.core.internal.FileAddress
import java.util.concurrent.atomic.AtomicBoolean

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

    override fun find(
        query: String,
        forwardIndex: Map<FileAddress, Set<String>>,
        reverseIndex: Map<String, Set<FileAddress>>,
        isActive: () -> Boolean,
    ) = sequence {
        val searchTokens = tokenize(query).toList()
        when (searchTokens.size) {
            0 -> {
                // index won't help us here, emit everything we have
                forwardIndex.keys.asSequence()
                    .takeWhile { isActive() }
                    .forEach { yield(it) }
            }

            1 -> {
                val searchToken = searchTokens[0].lowercase()
                val fullMatch = reverseIndex[searchToken]?.asSequence() ?: sequenceOf()
                val containsMatch = reverseIndex.entries
                    .asSequence()
                    .takeWhile { isActive() }
                    .filter { (token) -> token.contains(searchToken) }
                    .flatMap { (_, fas) -> fas }

                (fullMatch + containsMatch)
                    .takeWhile { isActive() }
                    .distinct()
                    .forEach { yield(it) }
            }

            2 -> {
                val (startToken, endToken) = searchTokens
                val startFullMatch = (reverseIndex[startToken]?.asSequence() ?: sequenceOf())
                    .filter { fa ->
                        val fileTokens = forwardIndex[fa] ?: mutableSetOf()
                        endToken in fileTokens || fileTokens.any { it.startsWith(endToken) }
                    }

                val endFullMatch = (reverseIndex[endToken]?.asSequence() ?: sequenceOf())
                    .filter { fa ->
                        val fileTokens = forwardIndex[fa] ?: mutableSetOf()
                        startToken in fileTokens || fileTokens.any { it.endsWith(startToken) }
                    }

                val bothPartialMatch = reverseIndex.entries.asSequence()
                    .takeWhile { isActive() }
                    .filter { (token) -> token.endsWith(startToken) }
                    .flatMap { (_, fas) -> fas.asSequence() }
                    .distinct()
                    .takeWhile { isActive() }
                    .filter { fa -> forwardIndex[fa]?.any { it.startsWith(endToken) } ?: false }

                (startFullMatch + endFullMatch + bothPartialMatch)
                    .takeWhile { isActive() }
                    .distinct()
                    .forEach { yield(it) }
            }

            else -> {
                val startToken = searchTokens.first()
                val endToken = searchTokens.last()
                val coreTokens = searchTokens.subList(1, searchTokens.lastIndex)
                coreTokens.map { reverseIndex[it] ?: setOf() }.minBy { it.size }
                    .asSequence()
                    .takeWhile { isActive() }
                    .filter { fa ->
                        val fileTokens = forwardIndex[fa] ?: mutableSetOf()

                        val coreTokensMatch = coreTokens.all { it in fileTokens }
                        if (!coreTokensMatch) return@filter false

                        val startTokenMatch =
                            startToken in fileTokens || fileTokens.any { it.endsWith(startToken) }
                        if (!startTokenMatch) return@filter false

                        val endTokenMatch =
                            endToken in fileTokens || fileTokens.any { it.startsWith(endToken) }
                        if (!endTokenMatch) return@filter false

                        return@filter true
                    }
                    .takeWhile { isActive() }
                    .distinct()
                    .forEach { yield(it) }
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
