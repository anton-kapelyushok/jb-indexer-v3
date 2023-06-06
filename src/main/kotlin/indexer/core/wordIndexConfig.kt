//package indexer.core
//
//import kotlinx.coroutines.ensureActive
//import kotlinx.coroutines.flow.flow
//import java.util.concurrent.atomic.AtomicBoolean
//import kotlin.coroutines.coroutineContext
//
//fun wordIndexConfig(
//    enableWatcher: Boolean = true,
//    handleWatcherError: (e: Throwable) -> Unit = {},
//    handleInitialFileSyncError: suspend (e: Throwable) -> Unit = {},
//) = object : IndexConfig {
//    private val regex = Regex("""\W+""")
//
//    override val enableLogging = AtomicBoolean(false)
//
//    override val enableWatcher: Boolean = enableWatcher
//
//    override fun tokenize(line: String): List<String> {
//        return line.split(regex).map { it.trim().lowercase() }
//            .filter { it.isNotEmpty() }
//    }
//
//    override suspend fun find(query: String, index: Index) = flow {
//        val searchTokens = tokenize(query).toList()
//        when (searchTokens.size) {
//            0 -> {
//                // index won't help us here, emit everything we have
//                val tokens = index.findTokensMatchingPredicate { true }
//                tokens
//                    .onEach { coroutineContext.ensureActive() }
//                    .flatMap { index.findFilesByToken(it) }
//                    .onEach { coroutineContext.ensureActive() }
//                    .forEach { emit(it) }
//
//                return@flow
//            }
//
//            1 -> {
//                val searchToken = searchTokens[0].lowercase()
//                index.findFilesByToken(searchToken).forEach { emit(it) }
//
//                index.findTokensMatchingPredicate { it.contains(searchToken) }
//                    .onEach { coroutineContext.ensureActive() }
//                    .flatMap { index.findFilesByToken(it) }
//                    .onEach { coroutineContext.ensureActive() }
//                    .forEach { emit(it) }
//            }
//
//            2 -> {
//                val (startToken, endToken) = searchTokens
//                // start full match
//                val startFullMatch = index.findFilesByToken(startToken).toSet()
//                val endFullMatch = index.findFilesByToken(startToken).toSet()
//            }
//        }
//    }
//
//    override fun matches(line: String, query: String): Boolean {
//        return line.contains(query)
//    }
//
//    override suspend fun handleWatcherError(e: Throwable) {
//        handleWatcherError(e)
//    }
//
//    override suspend fun handleInitialFileSyncError(e: Throwable) {
//        handleInitialFileSyncError(e)
//    }
//}