package indexer.core.test

import java.nio.file.Path
import kotlin.io.path.createDirectory
import kotlin.io.path.createFile

fun initializeDirectory(dir: Path): FolderSetup {
    val poupa = dir.child("poupa").apply { createFile() }
    val loupa = dir.child("loupa").apply { createFile() }

    val nestedDir = dir.child("nestedDir").apply { createDirectory() }
    val nestedPoupa = nestedDir.child("poupa").apply { createFile() }

    return FolderSetup(poupa, loupa, nestedDir, nestedPoupa)
}

data class FolderSetup(
    val poupa: Path,
    val loupa: Path,
    val nestedDir: Path,
    val nestedPoupa: Path,
)