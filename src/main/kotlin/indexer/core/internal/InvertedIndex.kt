package indexer.core.internal

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import it.unimi.dsi.fastutil.ints.IntArrayList
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap

class InvertedIndex {
    private var invertedIndexData = ConcurrentHashMap<String, IntArrayList>()
    private var lastFaRef = 0
    private var fileAddressByFileRef = mutableMapOf<Int, FileAddress>()
    private var fileRefByFileAddress = mutableMapOf<FileAddress, Int>()
    private var entriesCountByFileRef = Int2IntOpenHashMap()
    private var aliveEntries = 0
    private var totalEntries = 0

    val tokensCount get() = invertedIndexData.size
    val documentsCount get() = fileRefByFileAddress.size

    fun addOrUpdateDocument(fileAddress: FileAddress, tokens: Set<String>) {
        val prevEntriesCount = removePreviousFileRef(fileAddress)

        val faRef = lastFaRef++
        fileAddressByFileRef[faRef] = fileAddress
        fileRefByFileAddress[fileAddress] = faRef
        entriesCountByFileRef[faRef] = tokens.size

        aliveEntries = aliveEntries - prevEntriesCount + tokens.size
        totalEntries += tokens.size

        tokens.forEach { invertedIndexData[it] = (invertedIndexData[it] ?: IntArrayList(1)).apply { add(faRef) } }

        if (totalEntries != 0 && aliveEntries.toDouble() / totalEntries < 0.6) {
            compact()
        }
    }

    fun removeDocument(fileAddress: FileAddress) {
        val prevEntriesCount = removePreviousFileRef(fileAddress)
        aliveEntries -= prevEntriesCount
    }

    fun findFilesByToken(token: String): List<FileAddress> {
        return (invertedIndexData[token]?.mapNotNull { fileAddressByFileRef[it] } ?: listOf())
    }

    fun findTokensMatchingPredicate(matches: (String) -> Boolean): List<String> {
        return invertedIndexData.keys.filter { matches(it) }
    }

    fun reset() {
        invertedIndexData.clear()
        lastFaRef = 0
        fileAddressByFileRef.clear()
        fileRefByFileAddress.clear()
        entriesCountByFileRef.clear()
        aliveEntries = 0
        totalEntries = 0
    }

    fun compact() {
        val remappedKeys = mutableMapOf<Int, Int>()
        var lastKey = 1
        val start = System.currentTimeMillis()
        fileAddressByFileRef.keys.forEach { key ->
            remappedKeys[key] = lastKey++
        }

        lastFaRef = lastKey
        fileAddressByFileRef = fileAddressByFileRef.mapKeys { (k, _) -> remappedKeys[k]!! }.toMutableMap()
        fileRefByFileAddress = fileRefByFileAddress.mapValues { (_, v) -> remappedKeys[v]!! }.toMutableMap()
        entriesCountByFileRef = entriesCountByFileRef.mapKeysTo(Int2IntOpenHashMap()) { (k, _) -> remappedKeys[k] }

        val copyOfKeys = invertedIndexData.keys.toList()
        val chunksCount = Runtime.getRuntime().availableProcessors()
        val chunkSize = copyOfKeys.size / chunksCount + 1
        runBlocking(Dispatchers.Default) {
            for (i in 0 until chunksCount) {
                launch {
                    for (j in chunkSize * i until minOf(chunkSize * (i + 1), copyOfKeys.size)) {
                        val key = copyOfKeys[j]
                        val newData = invertedIndexData[key]!!.mapNotNullTo(IntArrayList()) { remappedKeys[it] }
                        if (newData.isEmpty) {
                            invertedIndexData.remove(key)
                        } else {
                            invertedIndexData[key] = newData
                        }
                    }
                }
            }
        }

        totalEntries = aliveEntries
    }

    private fun removePreviousFileRef(fa: FileAddress): Int {
        val prevRef = fileRefByFileAddress[fa]
        var prevEntriesCount = 0
        if (prevRef != null) {
            fileAddressByFileRef.remove(prevRef)
            fileRefByFileAddress.remove(fa)
            prevEntriesCount = entriesCountByFileRef.remove(prevRef)
        }
        return prevEntriesCount
    }
}