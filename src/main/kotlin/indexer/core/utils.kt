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
suspend fun invokeOnCancellation(close: suspend () -> Unit, use: suspend () -> Unit) {
    coroutineScope {
        val closerStartedLatch = CompletableDeferred<Unit>()
        val closerJob = launch {
            try {
                closerStartedLatch.complete(Unit)
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    close()
                }
            }
        }
        closerStartedLatch.await()
        use()
        closerJob.cancel()
    }
}