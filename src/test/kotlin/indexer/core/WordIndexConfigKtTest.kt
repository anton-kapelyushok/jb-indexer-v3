package indexer.core

import assertk.assertThat
import assertk.assertions.containsAll
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.containsNone
import indexer.core.internal.FileAddress
import indexer.core.internal.InvertedIndex
import indexer.core.test.fa
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class WordIndexConfigKtTest {

    private val config = wordIndexConfig()

    @Test
    fun `should tokenize strings`() {
        val tokens = config.tokenize("pOUpa    loupa volobueV mech    ")
        assertThat(tokens).containsExactlyInAnyOrder("poupa", "loupa", "volobuev", "mech")
    }

    @Test
    fun `should search one token query`() = runTest {
        val index = index(
            mapOf(
                fa("contains") to setOf("poupa"),
                fa("startsWith") to setOf("oupa"),
                fa("endsWith") to setOf("pou"),
                fa("equal") to setOf("ou"),
                fa("never") to setOf("volobuev"),
            )
        )

        config.find(index, "ou").toSet().let {
            assertThat(it).containsAll(fa("contains"), fa("startsWith"), fa("endsWith"), fa("equal"))
        }

        config.find(index, "ou ").toSet().let {
            assertThat(it).containsAll(fa("endsWith"), fa("equal"))
            assertThat(it).containsNone(fa("contains"), fa("startsWith"))
        }

        config.find(index, " ou").toSet().let {
            assertThat(it).containsAll(fa("startsWith"), fa("equal"))
            assertThat(it).containsNone(fa("contains"), fa("endsWith"))
        }

        config.find(index, " ou ").toSet().let {
            assertThat(it).containsAll(fa("equal"))
            assertThat(it).containsNone(fa("contains"), fa("startsWith"), fa("endsWith"))
        }
    }

    @Test
    fun `should search 2 tokens query`() = runTest {
        val index = index(
            mapOf(
                fa("1") to setOf("__ou", "pa__"),
                fa("2") to setOf("ou", "pa__"),
                fa("3") to setOf("__ou", "pa"),
                fa("4") to setOf("ou", "pa"),
                fa("5") to setOf("oupa"),
                fa("6") to setOf("paou"),
            )
        )

        config.find(index, "ou pa").toSet().let {
            assertThat(it).containsAll(fa("1"), fa("2"), fa("3"), fa("4"), fa("6"))
            assertThat(it).containsNone(fa("5"))
        }

        config.find(index, " ou pa").toSet().let {
            assertThat(it).containsAll(fa("2"), fa("4"))
            assertThat(it).containsNone(fa("1"), fa("3"), fa("5"), fa("6"))
        }

        config.find(index, "ou pa ").toSet().let {
            assertThat(it).containsAll(fa("3"), fa("4"))
            assertThat(it).containsNone(fa("1"), fa("2"), fa("5"), fa("6"))
        }

        config.find(index, " ou pa ").toSet().let {
            assertThat(it).containsAll(fa("4"))
            assertThat(it).containsNone(fa("1"), fa("2"), fa("3"), fa("4"), fa("5"), fa("6"))
        }
    }

    @Test
    fun `should search multiple tokens query`() = runTest {
        val index = index(
            mapOf(
                fa("1") to setOf("volobuev", "__ou", "pa__"),
                fa("2") to setOf("volobuev", "ou", "pa__"),
                fa("3") to setOf("volobuev", "__ou", "pa"),
                fa("4") to setOf("volobuev", "ou", "pa"),
                fa("5") to setOf("volobuev", "oupa"),
                fa("6") to setOf("volobuev", "paou"),

                fa("7") to setOf("__ou", "pa__"),
                fa("8") to setOf("ou", "pa__"),
                fa("9") to setOf("__ou", "pa"),
                fa("10") to setOf("ou", "pa"),
                fa("11") to setOf("oupa"),
                fa("12") to setOf("paou"),
            )
        )

        config.find(index, "ou volobuev pa").toSet().let {
            assertThat(it).containsAll(fa("1"), fa("2"), fa("3"), fa("4"), fa("6"))
            assertThat(it).containsNone(fa("7"), fa("8"), fa("9"), fa("10"), fa("11"), fa("12"))
        }

        config.find(index, " ou volobuev pa").toSet().let {
            assertThat(it).containsAll(fa("2"), fa("4"))
            assertThat(it).containsNone(fa("5"), fa("7"), fa("8"), fa("9"), fa("10"), fa("11"), fa("12"))
        }

        config.find(index, "ou volobuev pa ").toSet().let {
            assertThat(it).containsAll(fa("3"), fa("4"))
            assertThat(it).containsNone(fa("5"), fa("7"), fa("8"), fa("9"), fa("10"), fa("11"), fa("12"))
        }

        config.find(index, " ou volobuev pa ").toSet().let {
            assertThat(it).containsAll(fa("4"))
            assertThat(it).containsNone(fa("5"), fa("7"), fa("8"), fa("9"), fa("10"), fa("11"), fa("12"))
        }
    }

    private fun index(docs: Map<FileAddress, Set<String>>): Index {
        val invertedIndex = InvertedIndex()
        docs.forEach { (fa, tokens) -> invertedIndex.addOrUpdateDocument(fa, tokens) }
        return mockk {
            coEvery { findFilesByToken(any()) } answers { invertedIndex.findFilesByToken(arg(0)) }
            coEvery { findTokensMatchingPredicate(any()) } answers { invertedIndex.findTokensMatchingPredicate(arg(0)) }
        }
    }
}