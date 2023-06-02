package indexer.core

import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope

data class FileAddress(val path: String)

suspend fun index(indexRequests: ReceiveChannel<IndexRequest>) = coroutineScope {
    val startTime = System.currentTimeMillis()
    var watcherStartedTime: Long? = null
    var syncCompletedTime: Long? = null
    val fas = mutableMapOf<String, FileAddress>()
    val forwardIndex = mutableMapOf<FileAddress, MutableSet<String>>()
    val reverseIndex = mutableMapOf<String, MutableSet<FileAddress>>()
    val interner = mutableMapOf<String, String>()

    for (event in indexRequests) {
        if (enableLogging.get()) println("index: $event")
        when (event) {
            is UpdateFileContentRequest -> {
                val path = event.path.toFile().canonicalPath
                val tokens = event.tokens.map { token -> interner.computeIfAbsent(token) { it } }
                val fa = fas.computeIfAbsent(path) { FileAddress(it) }

                forwardIndex[fa]?.let { prevTokens ->
                    prevTokens.forEach { reverseIndex[it]?.remove(fa) }
                }

                forwardIndex[fa] = tokens.toMutableSet()
                tokens.forEach { reverseIndex[it] = (reverseIndex[it] ?: mutableSetOf()).apply { add(fa) } }
            }

            is RemoveFileRequest -> {
                val path = event.path.toFile().canonicalPath

                val fa = fas.computeIfAbsent(path) { FileAddress(it) }

                forwardIndex[fa]?.let { prevTokens ->
                    prevTokens.forEach { reverseIndex[it]?.remove(fa) }
                }
                forwardIndex.remove(fa)
            }

            // TODO: should probably be extracted
            is FindTokenRequest -> coroutineScope {
                val requestScope = this
                try {
                    val searchTokens = tokenize(event.query)

                    val onResult: suspend (FileAddress) -> Unit = { fa: FileAddress ->
                        event.onResult(fa)
                            .onFailure { e ->
                                if (enableLogging.get()) println("Search consumer failed with exception $e")
                                requestScope.cancel()
                            }
                    }

                    when (searchTokens.size) {
                        0 -> {
                            // index won't help us here, emit everything we have
                            forwardIndex.keys.asSequence().takeWhile { event.isConsumerAlive() }
                                .forEach { onResult(it) }
                        }

                        1 -> {
                            val query = searchTokens[0].lowercase()
                            val fullMatch = reverseIndex[query]?.asSequence() ?: sequenceOf()
                            val containsMatch = reverseIndex.entries
                                .asSequence()
                                // first isConsumerAlive - as often as possible
                                .takeWhile { event.isConsumerAlive() }
                                .filter { (token) -> token.contains(query) }
                                .flatMap { (_, fas) -> fas }

                            (fullMatch + containsMatch)
                                // second isConsumerAlive - before emitting result
                                .takeWhile { event.isConsumerAlive() }
                                .distinct()
                                .forEach { onResult(it) }
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
                                .takeWhile { event.isConsumerAlive() }
                                .filter { (token) -> token.endsWith(startToken) }
                                .flatMap { (_, fas) -> fas.asSequence() }
                                .distinct()
                                .takeWhile { event.isConsumerAlive() }
                                .filter { fa -> forwardIndex[fa]?.any { it.startsWith(endToken) } ?: false }

                            (startFullMatch + endFullMatch + bothPartialMatch)
                                .takeWhile { event.isConsumerAlive() }
                                .distinct()
                                .forEach { onResult(it) }
                        }

                        else -> {
                            val startToken = searchTokens.first()
                            val endToken = searchTokens.last()
                            val coreTokens = searchTokens.subList(1, searchTokens.lastIndex)
                            coreTokens.map { reverseIndex[it] ?: setOf() }.minBy { it.size }
                                .asSequence()
                                .takeWhile { event.isConsumerAlive() }
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
                                .takeWhile { event.isConsumerAlive() }
                                .distinct()
                                .forEach { onResult(it) }
                        }
                    }
                } catch (e: Throwable) {
                    event.onError(e)
                } finally {
                    event.onFinish()
                }
            }

            is StatusRequest -> {
                event.result.complete(
                    StatusResult(
                        forwardIndex.size,
                        reverseIndex.size,
                        watcherStartedTime?.let { it - startTime },
                        syncCompletedTime?.let { it - startTime },
                    )
                )
            }

            SyncCompletedMessage -> {
                syncCompletedTime = System.currentTimeMillis()
                println("Initial sync completed after ${syncCompletedTime - startTime} ms!")
            }

            WatcherStartedMessage -> {
                watcherStartedTime = System.currentTimeMillis()
                println("Watcher started after ${watcherStartedTime - startTime} ms!")
            }
        }
    }
}