package indexer.core.internal

import kotlinx.coroutines.*

/**
 * Invokes `close` on coroutine cancellation
 * Allows using closeable blocking resources in coroutine context.
 *
 * Example:
 *
 * withContext(Dispatchers.IO) {
 *     val watcher = DirectoryWatcher()
 *     invokeOnCancellation(this) { watcher.close() }
 *     watcher.watch()
 * }
 *
 * watcher.watch() is blocking, the only way to terminate it is by calling watcher.close()
 * invokeOnCancellation will call watcher.close() on coroutine cancellation
 *
 */
internal suspend fun invokeOnCancellation(scope: CoroutineScope, close: suspend () -> Unit) {
    val closerStartedLatch = CompletableDeferred<Unit>()
    scope.launch {
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
}