# FileChunker
Native application for splitting large text files and CSVs into smaller chunk files.

Usage from a command prompt:
```FileChunker.exe <filename> <chunkFilePrefix> <chunkSizeInMB>```

Optionally, a 4th parameter can be supplied to specify how much memory in MB to use as a chunk buffer for improved I/O performance.
