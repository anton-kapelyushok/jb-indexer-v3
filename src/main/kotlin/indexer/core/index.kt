package indexer.core

import kotlinx.coroutines.channels.ReceiveChannel
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

data class FileAddress(val path: String)

@OptIn(ExperimentalTime::class)
suspend fun index(indexRequests: ReceiveChannel<IndexRequest>) {

    val startTime = System.currentTimeMillis()
    var syncCompletedTime: Long? = null;
    val fas = mutableMapOf<String, FileAddress>()
    val forwardIndex = mutableMapOf<FileAddress, MutableSet<String>>()
    val reverseIndex = mutableMapOf<String, MutableSet<FileAddress>>()

    for (event in indexRequests) {
        if (enableLogging.get()) println("index: $event")
        when (event) {
            is UpdateFileContentRequest -> {
                val path = event.path.toFile().canonicalPath
                val tokens = event.tokens
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
                    val tokens = reverseIndex.entries.filter { (token) -> token.startsWith(query) }
                    val result = tokens
                        .asSequence()
                        .flatMap { (_, fas) -> fas }
                        .take(1000)
                        .toList()
                    output.complete(result)
                }
                println("found in $time")
            }

            is StatusRequest -> {
                event.result.complete(
                    StatusResult(
                        forwardIndex.size,
                        reverseIndex.size,
                        syncCompletedTime?.let { it - startTime }
                    )
                )
            }

            InitialSyncCompletedMessage -> {
                println("Initial sync completed!")
                syncCompletedTime = System.currentTimeMillis()

            }
        }
    }
}