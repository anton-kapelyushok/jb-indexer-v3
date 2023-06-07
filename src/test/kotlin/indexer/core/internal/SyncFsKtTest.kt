package indexer.core.internal

import assertk.assertThat
import assertk.assertions.containsAll
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import indexer.core.IndexConfig
import indexer.core.internal.FileEventSource.INITIAL_SYNC
import indexer.core.internal.FileEventSource.WATCHER
import indexer.core.internal.FileEventType.*
import indexer.core.test.*
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test
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

        // created files are catched
        val volobuevFile = workingDirectory.child("volobuev").apply { createFile() }
        receiveFileUpdated(WATCHER)
        receiveFileSyncEvent(5L, WATCHER, CREATE).let {
            assertThat(it).isEqualTo(volobuevFile.toFileAddress())
        }

        // updated files are also catched
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
}