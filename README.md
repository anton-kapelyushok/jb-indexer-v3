# File Indexer 2

```
Please implement a library for simple text search. Imagine, the library will then be used by other people.



This library is supposed to consist of two parts: text index builder and search query executor.



Text index builder should:



Be able to build a text index for a given folder in a file system. As an example of such a folder, you may use https://github.com/JetBrains/intellij-community repository. The type of the index doesn't matter much. For example, that might be a trigram index.
Provide the value of the progress while building the index. Clients of the library should be able to implement UI on the basis of that.
Build the index using several threads in parallel.
Be cancellable. It should be possible to interrupt indexing.
(Optional) Be incremental. It would be nice if the builder would be able to listen to the file system changes and update the index accordingly.


Search query executor should:



Find a position in files for a given string.
Be able to process search requests in parallel.


Please also cover the library with a set of unit-tests. Your code should not use third-party indexing libraries. To implement the library, you can use any JVM language and any build systems, but we would appreciate you choosing Kotlin and Gradle.
```

## Usage example

```kotlin
// launch index in current scope
val index = launchRessurectingIndex(scope, Path(dir), wordIndexConfig(watcherEnabled = true))

// wait until initial sync is completed (only if you want to)
index.statusFlow().filter { it.initialSyncTime != null }.first()

// execute search query
index.find("poupa loupa").collect { (path, lineNo, line) ->
    println("$path:$lineNo")
    println(line)
}

// get current status
println(index.status())

// cancel it
index.cancel()
```

## Design

```
+-----+                     +----+                            +------------+
|index|<---userRequests-----|repl| ---searchInFileRequests--->|searchInFile|
+-----+                     +----+                            +------------+
 ^  ^
 |  |
 |  |
 |  |
 |  +-------------------------+
 |                            |
 | indexUpdateRequests        | statusUpdates
 |                            |
+-------+                    +-------+
|indexer|<----file-events----|watcher|
+-------+                    +-------+

```

Conceptually it is 4 coroutines:

* index - contains index state
* watcher - synchronizes file system with index
* indexer - reads and parses files to pass to index (in parallel)
* searchInFile - finds query in file (in parallel)

### Design decisions

* I chose coroutines because they are fun
* index
    * uses ConcurrentHashMap to allow reads parallel with writes
        * considered HashMap with rw lock here, but decided against it - blocking writes during reads seemed like a bad
          decision
        * also considered HashMap with taking snapshots on read or write - it was too slow
        * also considered SegmentedIndex - also too slow
        * stores only file information (no positions in file) to fit in memory
    * listens for three channels in select to prioritize events: statusUpdates > userRequests > indexUpdateRequests
* watcher
    * decided to use `io.methvin:directory-watcher` library
        * I am using Mac, and `FileSystems.getDefault().newWatchService()` returns `PollingWatchService`, which is
          really slow
        * API is blocking and not exactly cooperative, so I have to make a lot of exercises to make it work
    * if watcher is on, I do the following for data consistency:
        * each FileEvent has clock associated with it
            * indexer runs in parallel - events might be out of order when they get to index coroutine
            * index coroutine discards update events if it has seen new ones
        * on initialize
            * start the watcher
            * wait until it is initialized, but buffer its events
            * emit all files in directory
            * start emitting watcher events (buffered and new)
* launchResurrectingIndex
    * creates index and recreates it if errors occur
        * the main purpose is to handle watcher overflow event
            * and the library I use just throws NPE when it happens))))0)
* search is executed as following
    * ask index for candidate files matching query
        * algorithm can be defined in IndexConfig
    * go through all candidate files, read them and check they match the query
    * wordIndex and trigramIndex are implemented
* client-library communication
    * events are sent to userRequests channel with CompletableDeferred field for result
    * channel.send and CompletableDeferred.await() are called in index context to avoid blocking forever when index
      breaks
* index status
    * events are originated in watcher
    * events are handled in index, but they are going through indexer
    * added direct statusUpdates channel between watcher and index to count unhandled events
        * was also considering sharing some AtomicLongs but decided against it
    * index reads statusUpdates in priority and updates its state
    * on major updates (index started, watcher started, initial sync completed) status update is pushed to statusFlow -
      it can be retrieved by calling index.statusFlow()
    * current status can be queried by calling index.status()