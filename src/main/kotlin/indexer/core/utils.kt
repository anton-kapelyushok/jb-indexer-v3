package indexer.core

import kotlinx.coroutines.*

/**
 * Invokes `close` on coroutine cancellation
 * Allows using closeable blocking resources in coroutine context.
 *
 * Example:
 *
 * withContext(Dispatchers.IO) {
 *     val watcher = DirectoryWatcher()
 *     withCancellationCallback({ watcher.close() }) {
 *         watcher.watch()
 *     }
 * }
 *
 * watcher.watch() is blocking, the only way to terminate it is by calling watcher.close()
 * withCancellationCallback will call watcher.close() on coroutine cancellation
 *
 */
suspend fun withCancellationCallback(close: suspend () -> Unit, use: suspend () -> Unit) {
    coroutineScope {
        val closerStarted = CompletableDeferred<Unit>()
        val closerJob = launch {
            try {
                closerStarted.complete(Unit)
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    close()
                }
            }
        }
        closerStarted.await()
        use()
        closerJob.cancel()
    }
}