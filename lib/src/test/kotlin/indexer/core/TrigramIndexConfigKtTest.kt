package indexer.core

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import indexer.core.test.fa
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds

class TrigramIndexConfigKtTest {
    private val config = trigramIndexConfig()

    @Test
    fun `should tokenize strings`() {
        val tokens = config.tokenize("poUPa loupa")
        assertThat(tokens).containsExactlyInAnyOrder("pou", "oup", "upa", "pa ", "a l", " lo", "lou", "oup", "upa")
    }

    @Test
    fun `should tokenize small strings`() {
        val tokens2 = config.tokenize("ab ")
        assertThat(tokens2).containsExactlyInAnyOrder("ab ")
        val tokens1 = config.tokenize("a  ")
        assertThat(tokens1).containsExactlyInAnyOrder("a  ")
        val tokens0 = config.tokenize("   ")
        assertThat(tokens0).containsExactlyInAnyOrder("   ")
    }

    @Test
    fun `should be able to search`() = runTest(timeout = 1.seconds) {
        val index = mockk<Index>() {
            coEvery { findFilesByToken("pou") } returns
                    listOf(fa("res1"), fa("res2"), fa("res3"), fa("res4"), fa("res6")) // no 5

            coEvery { findFilesByToken("oup") } returns
                    listOf(fa("res1"), fa("res2"), fa("res4"), fa("res5"), fa("res6")) // no 3

            coEvery { findFilesByToken("upa") } returns
                    listOf(fa("res1"), fa("res3"), fa("res5"), fa("res6")) // no 2 4
        }
        val found = config.find(index, "poUPa").toList()
        assertThat(found).containsExactlyInAnyOrder(fa("res1"), fa("res6"))
    }
}