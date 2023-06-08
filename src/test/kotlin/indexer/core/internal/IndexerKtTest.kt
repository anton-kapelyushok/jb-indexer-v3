package indexer.core.internal

import assertk.assertThat
import assertk.assertions.containsAll
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import indexer.core.IndexConfig
import indexer.core.test.child
import indexer.core.test.fa
import indexer.core.test.runTestWithFilesystem
import indexer.core.test.toFileAddress
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.io.path.createFile
import kotlin.io.path.writeText
import kotlin.time.Duration.Companion.seconds

class IndexerKtTest {
    private val indexConfig = mockk<IndexConfig>(relaxed = true) {
        every { tokenize(any()) } answers { (arg(0) as String).split(" ") }
    }
    private val fileSyncEvents = Channel<FileSyncEvent>()
    private val indexUpdateRequests = Channel<FileUpdateRequest>()

    @Test
    fun `should emit RemoveFileRequest on Remove event`() = runTest(timeout = 1.seconds) {
        val job = launch { indexer(indexConfig, fileSyncEvents, indexUpdateRequests) }

        fileSyncEvents.send(FileSyncEvent(10L, fa("doc1.txt"), FileEventSource.WATCHER, FileEventType.DELETE))

        val received = indexUpdateRequests.receive()

        assertThat(received).isInstanceOf(FileUpdateRequest.RemoveFileRequest::class)
        received as FileUpdateRequest.RemoveFileRequest
        assertThat(received.t).isEqualTo(10L)
        assertThat(received.fileAddress).isEqualTo(fa("doc1.txt"))
        job.cancel()
    }

    @Test
    fun `should emit UpdateFileRequest on Update event`() =
        runTestWithFilesystem { workingDirectory ->
            val job = launch { indexer(indexConfig, fileSyncEvents, indexUpdateRequests) }

            val file = workingDirectory.child("poupa")
            file.createFile()
            file.writeText(
                """
                volobuev gde tvoi mech
                a vot on
                """.trimIndent()
            )

            fileSyncEvents.send(
                FileSyncEvent(
                    t = 10L,
                    fileAddress = file.toFileAddress(),
                    source = FileEventSource.INITIAL_SYNC,
                    type = FileEventType.CREATE
                )
            )
            val received = indexUpdateRequests.receive()

            assertThat(received).isInstanceOf(FileUpdateRequest.UpdateFile::class)
            received as FileUpdateRequest.UpdateFile
            assertThat(received.t).isEqualTo(10L)
            assertThat(received.fileAddress).isEqualTo(file.toFileAddress())
            assertThat(received.tokens).containsAll("volobuev", "gde", "tvoi", "mech", "a", "vot", "onk")

            job.cancel()
        }

    @Test
    fun `should emit UpdateFileRequest with empty content on Update event on missing file`() =
        runTestWithFilesystem { workingDirectory ->
            val job = launch { indexer(indexConfig, fileSyncEvents, indexUpdateRequests) }

            val file = workingDirectory.child("poupa")

            fileSyncEvents.send(
                FileSyncEvent(
                    t = 10L,
                    fileAddress = file.toFileAddress(),
                    source = FileEventSource.INITIAL_SYNC,
                    type = FileEventType.CREATE
                )
            )
            val received = indexUpdateRequests.receive()

            assertThat(received).isInstanceOf(FileUpdateRequest.UpdateFile::class)
            received as FileUpdateRequest.UpdateFile
            assertThat(received.t).isEqualTo(10L)
            assertThat(received.fileAddress).isEqualTo(file.toFileAddress())
            assertThat(received.tokens).isEmpty()

            job.cancel()
        }

    @Test
    fun `should throw on error other than IllegalStateException`() =
        runTestWithFilesystem { workingDirectory ->
            supervisorScope {
                val job = async { indexer(indexConfig, fileSyncEvents, indexUpdateRequests) }
                val file = workingDirectory.child("poupa")
                file.createFile()
                file.writeText(
                    """
                volobuev gde tvoi mech
                a vot on
                """.trimIndent()
                )
                every { indexConfig.tokenize("a vot on") } throws IllegalStateException("not io exception")

                fileSyncEvents.send(
                    FileSyncEvent(
                        t = 10L,
                        fileAddress = file.toFileAddress(),
                        source = FileEventSource.INITIAL_SYNC,
                        type = FileEventType.CREATE
                    )
                )

                assertThrows<Throwable> { job.await() }
            }
        }
}