package indexer.core.internal

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import org.junit.jupiter.api.Test

class InvertedIndexTest {
    @Test
    fun `can add documents`() {
        val index = createIndex()

        index.addOrUpdateDocument(FileAddress("doc1.txt"), setOf("poupa", "loupa"))
        index.addOrUpdateDocument(FileAddress("doc2.txt"), setOf("loupa", "volobuev"))

        assertThat(index.documentsCount).isEqualTo(2)
        assertThat(index.aliveEntries).isEqualTo(4)
        assertThat(index.totalEntries).isEqualTo(4)

        assertThat(index.findFilesByToken("poupa")).containsExactlyInAnyOrder(fa("doc1.txt"))
        assertThat(index.findFilesByToken("loupa")).containsExactlyInAnyOrder(fa("doc1.txt"), fa("doc2.txt"))
        assertThat(index.findFilesByToken("volobuev")).containsExactlyInAnyOrder(fa("doc2.txt"))
    }

    @Test
    fun `can update documents`() {
        val index = createIndex()

        index.addOrUpdateDocument(FileAddress("doc1.txt"), setOf("poupa", "loupa"))
        index.addOrUpdateDocument(FileAddress("doc2.txt"), setOf("loupa", "volobuev"))
        index.addOrUpdateDocument(FileAddress("doc1.txt"), setOf("mech", "volobuev"))

        assertThat(index.aliveEntries).isEqualTo(4)
        assertThat(index.totalEntries).isEqualTo(6)

        assertThat(index.documentsCount).isEqualTo(2)
        assertThat(index.findFilesByToken("mech")).containsExactlyInAnyOrder(fa("doc1.txt"))
        assertThat(index.findFilesByToken("volobuev")).containsExactlyInAnyOrder(fa("doc1.txt"), fa("doc2.txt"))
        assertThat(index.findFilesByToken("loupa")).containsExactlyInAnyOrder(fa("doc2.txt"))
    }

    @Test
    fun `can remove documents`() {
        val index = createIndex()

        index.addOrUpdateDocument(FileAddress("doc1.txt"), setOf("poupa", "loupa"))
        index.addOrUpdateDocument(FileAddress("doc2.txt"), setOf("loupa", "volobuev"))
        index.removeDocument(FileAddress("doc1.txt"))

        assertThat(index.documentsCount).isEqualTo(1)
        assertThat(index.aliveEntries).isEqualTo(2)
        assertThat(index.totalEntries).isEqualTo(4)
        assertThat(index.findFilesByToken("poupa")).isEmpty()
        assertThat(index.findFilesByToken("loupa")).containsExactlyInAnyOrder(fa("doc2.txt"))
        assertThat(index.findFilesByToken("volobuev")).containsExactlyInAnyOrder(fa("doc2.txt"))
    }

    @Test
    fun `can compact`() {
        val index = createIndex()

        index.addOrUpdateDocument(FileAddress("doc1.txt"), setOf("poupa", "loupa"))
        index.addOrUpdateDocument(FileAddress("doc2.txt"), setOf("loupa", "volobuev"))
        index.removeDocument(FileAddress("doc1.txt"))

        assertThat(index.aliveEntries).isEqualTo(2)
        assertThat(index.totalEntries).isEqualTo(4)

        index.compact()

        assertThat(index.aliveEntries).isEqualTo(2)
        assertThat(index.totalEntries).isEqualTo(2)

        assertThat(index.findFilesByToken("poupa")).isEmpty()
        assertThat(index.findFilesByToken("loupa")).containsExactlyInAnyOrder(fa("doc2.txt"))
        assertThat(index.findFilesByToken("volobuev")).containsExactlyInAnyOrder(fa("doc2.txt"))
    }

    @Test
    fun `should compact on update if loadFactor threshold is reached`() {
        val index = createIndex(loadFactor = 0.75)
        index.addOrUpdateDocument(FileAddress("doc1.txt"), setOf("poupa", "loupa"))
        index.addOrUpdateDocument(FileAddress("doc2.txt"), setOf("loupa", "volobuev"))
        index.addOrUpdateDocument(FileAddress("doc1.txt"), setOf())

        assertThat(index.aliveEntries).isEqualTo(2)
        assertThat(index.totalEntries).isEqualTo(2)
    }

    private fun createIndex(loadFactor: Double = 0.0) = InvertedIndex(loadFactor)
    private fun fa(s: String) = FileAddress(s)
}