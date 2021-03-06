#define _CRT_SECURE_NO_WARNINGS
#include <cstdio>
#include <thread>
#include <string>
#include <vector>
#include <atomic>
#include <algorithm>

typedef uint64_t u64;

const size_t READ_BUF_SIZE = 16 * 1024 * 1024;
const size_t READ_AHEAD = 4;

char buf[READ_AHEAD][READ_BUF_SIZE];
size_t bufSize[READ_AHEAD];
std::atomic_uint64_t readCursor = 0;
std::atomic_uint64_t writeCursor = 0;

void FileReadAsync(FILE* file)
{
	for (;;)
	{
		bufSize[writeCursor % READ_AHEAD] = fread(buf[writeCursor % READ_AHEAD], 1, READ_BUF_SIZE, file);

		if (bufSize[writeCursor % READ_AHEAD] < READ_BUF_SIZE)
			break;

		++writeCursor;
		while (writeCursor - readCursor >= READ_AHEAD);
	}
}

int main(int argc, const char** argv)
{
	if (argc != 4)
	{
		printf("Expected usage: <filename> <chunkFilePrefix> <chunkSizeInMB> <chunkMemoryBufferInMB>");
		return -1;
	}

	const char* filename = argv[1];
	const char* chunkPrefix = argv[2];
	const char* chunkSizeStr = argv[3];
	const char* chunkBufMemStr = nullptr;
	if (argc == 5)
		chunkBufMemStr = argv[4];

	const char* fileExt = "txt";
	for (size_t i = strlen(filename); i > 0;)
	{
		--i;
		if (filename[i] == '.')
		{
			fileExt = filename + i;
			break;
		}
	}

	const bool isCSV = (std::strcmp(fileExt, "csv") == 0);
	std::string csvColumnHeaders;

	const u64 chunkSize = std::stoul(chunkSizeStr) * 1024ull * 1024ull;
	const u64 chunkBufMemSize = (chunkBufMemStr) ? std::max(READ_BUF_SIZE * 2, std::stoul(chunkBufMemStr) * 1024ull * 1024ull) : READ_BUF_SIZE * 2;

	FILE* src = fopen(filename, "rb");
	int error = errno;
	bufSize[writeCursor] = fread(buf[writeCursor], 1, READ_BUF_SIZE, src);
	writeCursor = 1;

	std::thread asyncFileThread = std::thread(FileReadAsync, src);

	std::vector<char> chunkBuf;
	chunkBuf.reserve(chunkBufMemSize + 2);

	size_t currentChunkSize = 0;
	size_t chunkIdx = 0;
	FILE* chunkFile = fopen((std::string(chunkPrefix) + std::to_string(chunkIdx) + fileExt).c_str(), "wb");
	u64 mostRecentNewLine = 0;
	u64 prevRecentNewLine = 0;
	for (;;)
	{
		size_t readSize = bufSize[readCursor % READ_AHEAD];
		const char* readBuf = buf[readCursor % READ_AHEAD];

		for (size_t i = 0; i < readSize; ++i)
		{
			char bufChar = readBuf[i];
			if (bufChar == '\n' || bufChar == '\r')
			{
				if (chunkBuf.back() == '\n')
					continue;

				chunkBuf.push_back('\r');
				chunkBuf.push_back('\n');
				prevRecentNewLine = mostRecentNewLine;
				mostRecentNewLine = chunkBuf.size();

				if (isCSV && csvColumnHeaders.empty())
					csvColumnHeaders.assign(chunkBuf.data(), chunkBuf.size());
			}
			else
			{
				chunkBuf.push_back(bufChar);
			}

			if (currentChunkSize + chunkBuf.size() > chunkSize)
			{
				if (prevRecentNewLine)
				{
					fwrite(chunkBuf.data(), 1, prevRecentNewLine + 1, chunkFile);
					chunkBuf.erase(chunkBuf.begin(), chunkBuf.begin() + prevRecentNewLine + 1);
				}
				
				mostRecentNewLine -= prevRecentNewLine;
				prevRecentNewLine = 0;
				fclose(chunkFile);

				printf("Finished writing %s\n", (std::string(chunkPrefix) + std::to_string(chunkIdx)).c_str());
				++chunkIdx;
				currentChunkSize = 0;
				chunkFile = fopen((std::string(chunkPrefix) + std::to_string(chunkIdx) + fileExt).c_str(), "wb");
				if (isCSV)
					chunkBuf.insert(chunkBuf.begin(), csvColumnHeaders.begin(), csvColumnHeaders.end());
			}

			if (chunkBuf.size() >= chunkBufMemSize)
			{
				fwrite(chunkBuf.data(), 1, mostRecentNewLine + 1, chunkFile);
				currentChunkSize += mostRecentNewLine + 1;
				chunkBuf.erase(chunkBuf.begin(), chunkBuf.begin() + mostRecentNewLine + 1);
				prevRecentNewLine = 0;
				mostRecentNewLine = 0;
			}
		}

		if (readSize < READ_BUF_SIZE)
			break;

		++readCursor;
		while (readCursor == writeCursor);
	}
	if (chunkBuf.size())
		fwrite(chunkBuf.data(), 1, chunkBuf.size(), chunkFile);
	fclose(chunkFile);
	
	asyncFileThread.join();

	return 0;
}