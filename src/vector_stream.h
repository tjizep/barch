//
// Created by teejip on 4/1/25.
//

#ifndef VECTOR_STREAM_H
#define VECTOR_STREAM_H
#include "sastam.h"
#include <vector>
#include <algorithm>
struct vector_stream
{
	size_t pos {};
	std::vector<uint8_t, heap::allocator<uint8_t>> buf{};
	void write(const char *data, size_t size)
    {
		if (pos > buf.size())
		{
			buf.resize(pos);
		}
		buf.insert(buf.end(), data, data + size);
		pos = buf.size();
    }
	void read(char *data, size_t size)
	{
		if (pos + size > buf.size())
		{
			throw std::out_of_range("vector_stream::read");
		}
		std::copy(data, data+size, &buf[pos]);
		pos += size;
	}
	[[nodiscard]] size_t tellg( ) const
	{
		return pos;
	}
	[[nodiscard]] size_t tellp( ) const
	{
		return pos;
	}
	void seek(size_t to)
	{
		pos = to;
	}
	void clear()
	{
		pos = 0;
		buf.clear();
	}
	bool good() const
	{
		return pos <= buf.size();
	}
};
#endif //VECTOR_STREAM_H
