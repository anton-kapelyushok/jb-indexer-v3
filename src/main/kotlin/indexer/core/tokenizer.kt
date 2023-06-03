package indexer.core


internal val regex = Regex("""\W+""")
internal fun tokenize(s: String): List<String> {
    return s.split(regex).map { it.trim().lowercase() }
        .filter { it.isNotEmpty() }
}

// search does not work properly for trigrams tokenizer when query is < 3 symbols
//fun tokenize(s: String): List<String> {
//    return s.lowercase()
//        .windowed(3)
//}