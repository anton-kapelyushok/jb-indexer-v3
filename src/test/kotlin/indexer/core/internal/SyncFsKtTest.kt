package indexer.core.internal

import assertk.assertThat
import assertk.assertions.containsAll
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isTrue
import com.google.common.collect.Interner
import indexer.core.IndexConfig
import indexer.core.internal.FileEventSource.INITIAL_SYNC
import indexer.core.internal.FileEventSource.WATCHER
import indexer.core.internal.FileEventType.*
import indexer.core.test.*
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.createFile
import kotlin.io.path.deleteExisting
import kotlin.io.path.writeText
import kotlin.time.Duration.Companion.seconds

class SyncFsKtTest {
    private val indexConfig = mockk<IndexConfig>(relaxed = true) {
        every { enableWatcher } returns true
    }

    private val statusUpdates = Channel<StatusUpdate>()
    private val fileSyncEvents = Channel<FileSyncEvent>()

    @Test
    fun `should initialize and run with correct events`() = runTestWithFilesystem(3.seconds) { workingDirectory ->
        val dirSetup = initializeDirectory(workingDirectory)
        val job = launch { syncFs(indexConfig, workingDirectory, fileSyncEvents, statusUpdates) }

        val emittedFiles = mutableListOf<FileAddress>()

        // watcher initialization
        receiveWatcherDiscoveredFileDuringInitialization()
        receiveWatcherDiscoveredFileDuringInitialization()
        receiveWatcherDiscoveredFileDuringInitialization()
        receiveWatcherStarted()

        // file sync start
        receiveFileUpdated(INITIAL_SYNC)
        emittedFiles += receiveFileSyncEvent(1L, INITIAL_SYNC, CREATE)
        receiveFileUpdated(INITIAL_SYNC)
        emittedFiles += receiveFileSyncEvent(2L, INITIAL_SYNC, CREATE)
        receiveFileUpdated(INITIAL_SYNC)

        // some file updated during initialization
        emittedFiles[0].toPath().deleteExisting()

        emittedFiles += receiveFileSyncEvent(3L, INITIAL_SYNC, CREATE)
        receiveAllFilesDiscovered()

        assertThat(emittedFiles).containsAll(
            dirSetup.loupa.toFileAddress(),
            dirSetup.poupa.toFileAddress(),
            dirSetup.nestedPoupa.toFileAddress(),
        )

        // file updated during initialization was postponed until now
        receiveFileUpdated(WATCHER)
        receiveFileSyncEvent(4L, WATCHER, DELETE).let {
            assertThat(it).isEqualTo(emittedFiles[0])
        }

        // created files are captured
        val volobuevFile = workingDirectory.child("volobuev").apply { createFile() }
        receiveFileUpdated(WATCHER)
        receiveFileSyncEvent(5L, WATCHER, CREATE).let {
            assertThat(it).isEqualTo(volobuevFile.toFileAddress())
        }

        // updated files are also captured
        dirSetup.nestedPoupa.writeText("some really important content update")
        receiveFileUpdated(WATCHER)
        receiveFileSyncEvent(6L, WATCHER, MODIFY).let {
            assertThat(it).isEqualTo(dirSetup.nestedPoupa.toFileAddress())
        }
        job.cancel()
    }

    @Test
    fun `should cancel during initial file sync`() = runTestWithFilesystem { workingDirectory ->
        initializeDirectory(workingDirectory)
        val job = launch { syncFs(indexConfig, workingDirectory, fileSyncEvents, statusUpdates) }

        // watcher initialization
        receiveWatcherDiscoveredFileDuringInitialization()
        receiveWatcherDiscoveredFileDuringInitialization()
        receiveWatcherDiscoveredFileDuringInitialization()
        receiveWatcherStarted()

        // file sync start
        receiveFileUpdated(INITIAL_SYNC)
        receiveFileSyncEvent(1L, INITIAL_SYNC, CREATE)

        // there are 2 more files, but cancel!
        job.cancel()
    }

    @Test
    fun `should cancel during watcher initializing`() = runTestWithFilesystem { workingDirectory ->
        initializeDirectory(workingDirectory)
        val job = launch { syncFs(indexConfig, workingDirectory, fileSyncEvents, statusUpdates) }

        // watcher initialization
        receiveWatcherDiscoveredFileDuringInitialization()
        receiveWatcherDiscoveredFileDuringInitialization()

        // there is one more file, but cancel!
        job.cancel()
    }

    @Test
    fun `emitted file addresses should have referential equality`() = runTestWithFilesystem { workingDirectory ->
        val poupa = workingDirectory.child("poupa").apply { createFile() }
        val job = launch { syncFs(indexConfig, workingDirectory, fileSyncEvents, statusUpdates) }

        // watcher initialization
        receiveWatcherDiscoveredFileDuringInitialization()
        receiveWatcherStarted()

        // file sync start
        receiveFileUpdated(INITIAL_SYNC)
        val firstFa = receiveFileSyncEvent(1L, INITIAL_SYNC, CREATE)
        receiveAllFilesDiscovered()

        poupa.deleteExisting()
        receiveFileUpdated(WATCHER)
        val secondFa = receiveFileSyncEvent(2L, WATCHER, DELETE)

        assertThat(firstFa === secondFa).isTrue()

        job.cancel()
    }

    @Test
    fun `should be able to reinitialize when watcher error happens after initial file sync`() =
        runTestWithFilesystem { workingDirectory ->
            initializeDirectory(workingDirectory)
            val noopWatcherSetup = noopWatcherSetup()

            val job = launch {
                syncFs(
                    cfg = indexConfig,
                    dir = workingDirectory,
                    fileSyncEvents = fileSyncEvents,
                    statusUpdates = statusUpdates,
                    watcher = noopWatcherSetup.watcher
                )
            }

            var watcherStartedLatch = noopWatcherSetup.controls.watcherStartedLatch.await()
            val breakWatcher = noopWatcherSetup.controls.breakWatcher

            // watcher initialization
            completeWatcherInitialization(watcherStartedLatch)

            // start file sync
            receiveFileUpdated(INITIAL_SYNC)
            receiveFileSyncEvent(1L, INITIAL_SYNC, CREATE)
            receiveFileUpdated(INITIAL_SYNC)

            // watcher breaks
            breakWatcher.complete(IllegalStateException("poupa"))
            noopWatcherSetup.controls.reset()
            receiveFileSyncFailed().also {
                assertThat(it.t).isEqualTo(3L)
            }

            // watcher restarts
            watcherStartedLatch = noopWatcherSetup.controls.watcherStartedLatch.await()

            // fs reinitializes
            completeWatcherInitialization(watcherStartedLatch)
            completeInitialFileSync(3L)

            job.cancel()
        }

    @Test
    fun `should be able to reinitialize when watcher error happens before initial file sync`() =
        runTestWithFilesystem { workingDirectory ->
            initializeDirectory(workingDirectory)
            val noopWatcherSetup = noopWatcherSetup()

            val job = launch {
                syncFs(
                    cfg = indexConfig,
                    dir = workingDirectory,
                    fileSyncEvents = fileSyncEvents,
                    statusUpdates = statusUpdates,
                    watcher = noopWatcherSetup.watcher
                )
            }

            val breakWatcher = noopWatcherSetup.controls.breakWatcher

            // watcher initialization
            launch { sendWatcherDiscoveredFileDuringInitialization() }
            receiveWatcherDiscoveredFileDuringInitialization()
            launch { sendWatcherDiscoveredFileDuringInitialization() }
            receiveWatcherDiscoveredFileDuringInitialization()

            // watcher breaks
            breakWatcher.complete(IllegalStateException("poupa"))
            noopWatcherSetup.controls.reset()
            receiveFileSyncFailed().also {
                assertThat(it.t).isEqualTo(1L)
            }

            // watcher restarts
            val watcherStartedLatch = noopWatcherSetup.controls.watcherStartedLatch.await()

            // fs reinitializes
            completeWatcherInitialization(watcherStartedLatch)
            completeInitialFileSync(1L)

            job.cancel()
        }

    private suspend fun completeWatcherInitialization(
        watcherStartedLatch: CompletableDeferred<Unit>,
    ) = coroutineScope {
        launch { sendWatcherDiscoveredFileDuringInitialization() }
        receiveWatcherDiscoveredFileDuringInitialization()
        launch { sendWatcherDiscoveredFileDuringInitialization() }
        receiveWatcherDiscoveredFileDuringInitialization()
        launch { sendWatcherDiscoveredFileDuringInitialization() }
        receiveWatcherDiscoveredFileDuringInitialization()

        launch { sendWatcherStarted() }
        receiveWatcherStarted()
        watcherStartedLatch.complete(Unit)
    }

    private suspend fun completeInitialFileSync(
        lastT: Long,
    ) {
        receiveFileUpdated(INITIAL_SYNC)
        receiveFileSyncEvent(lastT + 1, INITIAL_SYNC, CREATE)
        receiveFileUpdated(INITIAL_SYNC)
        receiveFileSyncEvent(lastT + 2, INITIAL_SYNC, CREATE)
        receiveFileUpdated(INITIAL_SYNC)
        receiveFileSyncEvent(lastT + 3, INITIAL_SYNC, CREATE)
        receiveAllFilesDiscovered()
    }

    private suspend fun receiveFileUpdated(source: FileEventSource) {
        statusUpdates.receive().let {
            assertThat(it).isInstanceOf(StatusUpdate.FileUpdated::class)
            it as StatusUpdate.FileUpdated
            assertThat(it.source).isEqualTo(source)
        }
    }

    private suspend fun receiveFileSyncEvent(t: Long, source: FileEventSource, type: FileEventType): FileAddress {
        return fileSyncEvents.receive().let {
            assertThat(it.t).isEqualTo(t)
            assertThat(it.source).isEqualTo(source)
            assertThat(it.type).isEqualTo(type)
            it.fileAddress
        }
    }

    private suspend fun sendWatcherDiscoveredFileDuringInitialization() {
        statusUpdates.send(StatusUpdate.WatcherDiscoveredFileDuringInitialization)
    }

    private suspend fun sendWatcherStarted() {
        statusUpdates.send(StatusUpdate.WatcherStarted)
    }

    private suspend fun receiveWatcherDiscoveredFileDuringInitialization() {
        statusUpdates.receive().let {
            assertThat(it).isInstanceOf(StatusUpdate.WatcherDiscoveredFileDuringInitialization::class)
        }
    }

    private suspend fun receiveWatcherStarted() {
        statusUpdates.receive().let {
            assertThat(it).isInstanceOf(StatusUpdate.WatcherStarted::class)
        }
    }

    private suspend fun receiveAllFilesDiscovered() {
        statusUpdates.receive().let {
            assertThat(it).isInstanceOf(StatusUpdate.AllFilesDiscovered::class)
        }
    }

    private suspend fun receiveFileSyncFailed(): StatusUpdate.FileSyncFailed {
        val update = statusUpdates.receive().also {
            assertThat(it).isInstanceOf(StatusUpdate.FileSyncFailed::class)
        }
        return update as StatusUpdate.FileSyncFailed
    }

    private class NoopWatcherSetup(
        val watcher: Watcher,
        val controls: WatcherControls,
    ) {
    }

    private class WatcherControls(
        var fileSyncEvents: CompletableDeferred<SendChannel<FileSyncEvent>> = CompletableDeferred(),
        var watcherStartedLatch: CompletableDeferred<CompletableDeferred<Unit>> = CompletableDeferred(),
        var breakWatcher: CompletableDeferred<Throwable> = CompletableDeferred(),
    ) {
        fun reset() {
            fileSyncEvents = CompletableDeferred()
            watcherStartedLatch = CompletableDeferred()
            breakWatcher = CompletableDeferred()
        }
    }

    private fun noopWatcherSetup(): NoopWatcherSetup {
        val controls = WatcherControls()
        val watcher = object : Watcher {
            override suspend fun watch(
                dir: Path,
                clock: AtomicLong,
                faInterner: Interner<FileAddress>,
                fileSyncEvents: SendChannel<FileSyncEvent>,
                statusUpdates: SendChannel<StatusUpdate>,
                watcherStartedLatch: CompletableDeferred<Unit>,
            ): Result<Any> {
                controls.fileSyncEvents.complete(fileSyncEvents)
                controls.watcherStartedLatch.complete(watcherStartedLatch)
                return Result.failure(controls.breakWatcher.await())
            }
        }
        return NoopWatcherSetup(watcher, controls)
    }
}