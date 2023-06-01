package indexer.core

import kotlinx.coroutines.channels.ReceiveChannel
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

data class FileAddress(val path: String)

@OptIn(ExperimentalTime::class)
suspend fun index(indexRequests: ReceiveChannel<IndexRequest>) {
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

            is FindTokenRequest -> {
                val time = measureTime {
                    val query = event.query
                    val output = event.possibleResults

                    val fullMatch = reverseIndex[query]?.asSequence() ?: sequenceOf()
                    val containsMatch = reverseIndex.entries
                        .asSequence()
                        .filter { (token) -> token.startsWith(query) }
                        .flatMap { (_, fas) -> fas }

                    val result = (fullMatch + containsMatch)
                        .distinct()
                        .take(5)
                        .toList()

                    output.complete(result)
                }
                if (enableLogging.get()) println("index: found in $time")
            }

            is FindTokenRequest2 -> {
                try {
                    val query = event.query
                    val fullMatch = reverseIndex[query]?.asSequence() ?: sequenceOf()
                    val containsMatch = reverseIndex.entries
                        .asSequence()
                        .takeWhile { event.isConsumerAlive() }
                        .filter { (token) -> token.startsWith(query) }
                        .flatMap { (_, fas) -> fas }

                    val result = (fullMatch + containsMatch)
                        .distinct()

                    result
                        .takeWhile { event.isConsumerAlive() }
                        .forEach {
                            event.onResult(it)
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