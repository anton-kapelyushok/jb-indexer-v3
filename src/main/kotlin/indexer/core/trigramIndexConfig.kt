package indexer.core

import indexer.core.internal.FileAddress
import java.util.concurrent.atomic.AtomicBoolean

fun trigramIndexConfig(
    enableWatcher: Boolean = true,
    handleWatcherError: suspend (e: Throwable) -> Unit = {},
    handleInitialFileSyncError: suspend (e: Throwable) -> Unit = {},
) = object : IndexConfig {
    override val enableLogging = AtomicBoolean(false)

    override val enableWatcher: Boolean = enableWatcher

    override fun tokenize(line: String): List<String> {
        return line.lowercase().windowed(3)


//            .map {
//            val chars = it.toCharArray()
//            val c0 = chars[0].code
//            val c1 = chars[1].code
//            val c2 = chars[2].code
//
//
//            (c0.toLong() shl 16 * 2) + (c1 shl 16 * 1) + c2
//        }
    }

    override fun find(
        query: String,
        forwardIndex: Map<FileAddress, Set<String>>,
        reverseIndex: Map<String, Set<FileAddress>>,
        isActive: () -> Boolean,
    ) = sequence {
        when (query.length) {
            0 -> {
                // everything matches
                forwardIndex.keys.asSequence()
                    .takeWhile { isActive() }
                    .forEach { yield(it) }
                return@sequence
            }

            1, 2 -> {
                // files with trigrams containing query match
                reverseIndex.keys.asSequence()
                    .takeWhile { isActive() }
                    .filter { it.contains(query) }
                    .flatMap { reverseIndex[it]?.asSequence() ?: sequenceOf() }
                    .takeWhile { isActive() }
                    .forEach { yield(it) }
                return@sequence
            }
        }

//        val searchTokens = tokenize(query).toList()
        val searchTokens = listOf<String>()

        searchTokens.map { reverseIndex[it] ?: setOf() }.minBy { it.size }
            .asSequence()
            .takeWhile { isActive() }
            .filter { fa ->
                val fileTokens = forwardIndex[fa] ?: mutableSetOf()

                val coreTokensMatch = searchTokens.all { it in fileTokens }
                if (!coreTokensMatch) return@filter false
                return@filter true
            }
            .takeWhile { isActive() }
            .forEach { yield(it) }
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
