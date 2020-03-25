# FileChunker
Native application for splitting large text files and CSVs into smaller chunk files. Chunk files are broken on text line boundaries and CRLF is the newline separater used in the resulting output.

If the input file has a .csv extension, chunk files will contain the appropriate column headers.

Usage from a command prompt:
```FileChunker.exe <filename> <chunkFilePrefix> <chunkSizeInMB>```

Optionally, a 4th parameter can be supplied to specify how much memory in MB to use as a chunk buffer for improved I/O performance. The memory buffer should ideally be the same size as the chunk size.

Fractional MB values are not supported.