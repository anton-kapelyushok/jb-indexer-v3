package indexer.core.internal

import assertk.assertThat
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import indexer.core.IndexState
import indexer.core.IndexStatusUpdate
import indexer.core.test.fa
import io.mockk.clearMocks
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class IndexManagerTest {

    private var clock = 1L
    private lateinit var indexManager: IndexManager
    private lateinit var invertedIndex: InvertedIndex
    private lateinit var emittedEvents: MutableList<IndexStatusUpdate>

    @BeforeEach
    fun setup() {
        val indexManagerSetup = indexManagerSetup()
        indexManager = indexManagerSetup.indexManager
        invertedIndex = indexManagerSetup.invertedIndex
        emittedEvents = indexManagerSetup.emittedEvents
    }

    @Test
    fun `should track file events`() {
        // found 1 file
        indexManager.handleFileUpdated()
        assertThat(indexManager.status().handledFileEvents).isEqualTo(0L)
        assertThat(indexManager.status().totalFileEvents).isEqualTo(1L)

        // found 2 file
        indexManager.handleFileUpdated()
        assertThat(indexManager.status().handledFileEvents).isEqualTo(0L)
        assertThat(indexManager.status().totalFileEvents).isEqualTo(2L)

        // read 1 file
        indexManager.handleUpdateFileContentRequest(
            FileUpdateRequest.UpdateFile(clock++, fa("doc1.txt"), setOf(), FileEventSource.INITIAL_SYNC)
        )
        assertThat(indexManager.status().handledFileEvents).isEqualTo(1L)
        assertThat(indexManager.status().totalFileEvents).isEqualTo(2L)
        assertThat(emittedEvents).isEmpty()

        // read 2 file
        indexManager.handleUpdateFileContentRequest(
            FileUpdateRequest.UpdateFile(clock++, fa("doc2.txt"), setOf(), FileEventSource.INITIAL_SYNC)
        )
        assertThat(indexManager.status().handledFileEvents).isEqualTo(2L)
        assertThat(indexManager.status().totalFileEvents).isEqualTo(2L)

        // did not receive AllFilesDiscoveredEvent received, there might be more, not in sync
        assertThat(indexManager.status().isInSync()).isEqualTo(false)
        assertThat(emittedEvents).isEmpty()

        // found 3 file - there were more!
        indexManager.handleFileUpdated()

        // finished initial file sync, still 1 event inflight
        indexManager.handleAllFilesDiscovered()
        assertThat(indexManager.status().isInSync()).isEqualTo(false)
        assertThat(emittedEvents.size).isEqualTo(1)
        assertThat(emittedEvents.removeLast()).isInstanceOf(IndexStatusUpdate.AllFilesDiscovered::class)

        // read 3 file, no more in flight - we are in sync
        indexManager.handleUpdateFileContentRequest(
            FileUpdateRequest.UpdateFile(clock++, fa("doc3.txt"), setOf(), FileEventSource.INITIAL_SYNC)
        )
        assertThat(indexManager.status().isInSync()).isEqualTo(true)
        assertThat(emittedEvents.size).isEqualTo(1)
        assertThat(emittedEvents.removeLast()).isInstanceOf(IndexStatusUpdate.IndexInSync::class)

        // some file updated, out of sync again
        indexManager.handleFileUpdated()
        assertThat(indexManager.status().isInSync()).isEqualTo(false)
        assertThat(emittedEvents.size).isEqualTo(1)
        assertThat(emittedEvents.removeLast()).isInstanceOf(IndexStatusUpdate.IndexOutOfSync::class)

        // handled an update - in sync now
        indexManager.handleRemoveFileRequest(
            FileUpdateRequest.RemoveFileRequest(clock++, fa("doc3.txt"), FileEventSource.WATCHER)
        )
        assertThat(emittedEvents.size).isEqualTo(1)
        assertThat(emittedEvents.removeLast()).isInstanceOf(IndexStatusUpdate.IndexInSync::class)
    }

    @Test
    fun `should emit AllFilesDiscovered and IndexInSync events on empty directory`() {
        indexManager.handleAllFilesDiscovered()
        assertThat(emittedEvents.size).isEqualTo(2)
        assertThat(emittedEvents.removeFirst()).isInstanceOf(IndexStatusUpdate.AllFilesDiscovered::class)
        assertThat(emittedEvents.removeFirst()).isInstanceOf(IndexStatusUpdate.IndexInSync::class)
    }

    @Test
    fun `should ignore stale updates`() {
        val fa = FileAddress("doc1.txt")
        // t=1
        indexManager.handleRemoveFileRequest(FileUpdateRequest.RemoveFileRequest(2L, fa, FileEventSource.WATCHER))
        assertThat(indexManager.status().handledFileEvents).isEqualTo(1L)
        verify { invertedIndex.removeDocument(fa) }
        clearMocks(invertedIndex)

        // t=1 is less than t=2, ignore
        indexManager.handleUpdateFileContentRequest(
            FileUpdateRequest.UpdateFile(1L, fa, setOf(), FileEventSource.INITIAL_SYNC)
        )
        assertThat(indexManager.status().handledFileEvents).isEqualTo(2L)
        verify(exactly = 0) { invertedIndex.addOrUpdateDocument(fa, any()) }
        clearMocks(invertedIndex)

        // t=3 is greater than t=2, handle
        indexManager.handleUpdateFileContentRequest(
            FileUpdateRequest.UpdateFile(3L, fa, setOf(), FileEventSource.WATCHER)
        )
        assertThat(indexManager.status().handledFileEvents).isEqualTo(3L)
        verify { invertedIndex.addOrUpdateDocument(fa, any()) }
        clearMocks(invertedIndex)
    }

    @Test
    fun `should report total file events from watcher until all files are discovered by initial file sync`() {
        repeat(5) {
            indexManager.handleWatcherDiscoveredFileDuringInitialization()
        }
        repeat(4) {
            indexManager.handleFileUpdated()
        }

        // watcher events is greater than file sync -> return watcher count
        assertThat(indexManager.status().totalFileEvents).isEqualTo(5)

        // all files discovered, we know for sure that file sync events are correct
        indexManager.handleAllFilesDiscovered()
        assertThat(indexManager.status().totalFileEvents).isEqualTo(4)
    }

    @Test
    fun `should report total file events from handleFileUpdated() when its amount exceeds amount discovered by watcher`() {
        // only watcher events -> return watcher
        repeat(5) {
            indexManager.handleWatcherDiscoveredFileDuringInitialization()
        }
        assertThat(indexManager.status().totalFileEvents).isEqualTo(5)

        // 5 watcher events, 3 file sync -> return watcher
        repeat(3) {
            indexManager.handleFileUpdated()
        }
        assertThat(indexManager.status().totalFileEvents).isEqualTo(5)

        // 5 watcher events, 6 file sync -> return file sync
        repeat(3) {
            indexManager.handleFileUpdated()
        }
        assertThat(indexManager.status().totalFileEvents).isEqualTo(6)
    }

    @Test
    fun `should reset state when file sync failed`() {
        // initialize normally
        indexManager.handleWatcherDiscoveredFileDuringInitialization()
        indexManager.handleWatcherDiscoveredFileDuringInitialization()
        indexManager.handleWatcherStarted()
        indexManager.handleFileUpdated()
        indexManager.handleUpdateFileContentRequest(
            FileUpdateRequest.UpdateFile(1L, FileAddress("doc1.txt"), setOf(), FileEventSource.INITIAL_SYNC)
        )
        indexManager.handleFileUpdated()
        indexManager.handleUpdateFileContentRequest(
            FileUpdateRequest.UpdateFile(2L, FileAddress("doc2.txt"), setOf(), FileEventSource.INITIAL_SYNC)
        )
        indexManager.handleAllFilesDiscovered()
        with(indexManager.status()) {
            assertThat(watcherStarted).isEqualTo(true)
            assertThat(allFileDiscovered).isEqualTo(true)
            assertThat(totalFileEvents).isEqualTo(2L)
            assertThat(handledFileEvents).isEqualTo(2L)
        }
        assertThat(emittedEvents.last()).isInstanceOf(IndexStatusUpdate.IndexInSync::class)
        emittedEvents.clear()

        // watcher failed
        indexManager.handleFileSyncFailed(StatusUpdate.FileSyncFailed(3L, IllegalStateException()))
        with(indexManager.status()) {
            assertThat(watcherStarted).isEqualTo(false)
            assertThat(allFileDiscovered).isEqualTo(false)
            assertThat(totalFileEvents).isEqualTo(0L)
            assertThat(handledFileEvents).isEqualTo(0L)
        }
        assertThat(emittedEvents.size).isEqualTo(1)
        assertThat(emittedEvents.removeLast()).isInstanceOf(IndexStatusUpdate.ReinitializingBecauseFileSyncFailed::class)
    }

    @Test
    fun `should ignore updates happened before watcher reset`() {
        // starting normally
        indexManager.handleWatcherDiscoveredFileDuringInitialization()
        indexManager.handleWatcherDiscoveredFileDuringInitialization()
        indexManager.handleWatcherStarted()
        indexManager.handleFileUpdated()

        // event is not stale, handling it
        indexManager.handleUpdateFileContentRequest(
            FileUpdateRequest.UpdateFile(1L, FileAddress("doc1.txt"), setOf(), FileEventSource.INITIAL_SYNC)
        )
        verify { invertedIndex.addOrUpdateDocument(FileAddress("doc1.txt"), any()) }
        clearMocks(invertedIndex)

        indexManager.handleFileUpdated()
        indexManager.handleAllFilesDiscovered()

        // watcher failed
        indexManager.handleFileSyncFailed(StatusUpdate.FileSyncFailed(3L, IllegalStateException()))

        // stale event received
        indexManager.handleUpdateFileContentRequest(
            FileUpdateRequest.UpdateFile(2L, FileAddress("doc2.txt"), setOf(), FileEventSource.INITIAL_SYNC)
        )
        verify(exactly = 0) { invertedIndex.addOrUpdateDocument(any(), any()) }
        clearMocks(invertedIndex)

        // reinitializing
        indexManager.handleWatcherDiscoveredFileDuringInitialization()
        indexManager.handleWatcherDiscoveredFileDuringInitialization()
        indexManager.handleWatcherStarted()
        indexManager.handleFileUpdated()

        // event happened after FileSyncFailed
        indexManager.handleUpdateFileContentRequest(
            FileUpdateRequest.UpdateFile(4L, FileAddress("doc1.txt"), setOf(), FileEventSource.INITIAL_SYNC)
        )
        verify { invertedIndex.addOrUpdateDocument(FileAddress("doc1.txt"), any()) }
        clearMocks(invertedIndex)
    }

    private fun indexManagerSetup(): IndexManagerSetup {
        val invertedIndex = mockk<InvertedIndex>(relaxed = true)
        val emittedEvents = mutableListOf<IndexStatusUpdate>()
        val indexManager = IndexManager(emitStatusUpdate = { emittedEvents += it }, invertedIndex = invertedIndex)
        return IndexManagerSetup(indexManager, invertedIndex, emittedEvents)
    }

    private data class IndexManagerSetup(
        val indexManager: IndexManager,
        val invertedIndex: InvertedIndex,
        val emittedEvents: MutableList<IndexStatusUpdate>,
    )

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun IndexManager.status(): IndexState {
        val result = CompletableDeferred<IndexState>()
        handleStatusRequest(UserRequest.Status(result))
        return result.getCompleted()
    }
}