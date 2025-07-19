//
// Created by teejip on 4/1/25.
//

#ifndef VECTOR_STREAM_H
#define VECTOR_STREAM_H
#include "sastam.h"
#include <vector>
#include <algorithm>
struct vector_stream  {
    size_t pos{};
    heap::std_vector<uint8_t> buf{};
    vector_stream() = default;
    vector_stream(const vector_stream&) = default;
    vector_stream& operator=(const vector_stream&) = default;

    vector_stream(vector_stream&& other) {
        pos = other.pos;
        buf = std::move(other.buf);
        other.pos = 0;
    }

    vector_stream& operator=(vector_stream&& other) {
        pos = other.pos;
        buf = std::move(other.buf);
        other.pos = 0;
        return *this;
    }
    bool empty() const {
        return buf.empty();
    }
    void write(const char *data, size_t size) {
        if (nullptr == data) {
            throw_exception<std::invalid_argument>("parameter null");
        }

        if (buf.size() < pos + size)
            buf.resize(pos + size);

        size_t at = pos;

        for (; at < pos + size; ++at) {
            buf[at] = *data;
            ++data;
        }

        pos = at;
        assert(pos <= buf.size());
    }

    void read(char *data, size_t size) {
        if (pos + size > buf.size()) {
            throw std::out_of_range("vector_stream::read");
        }
        memcpy(data, buf.data()+pos, size);
        pos += size;
    }

    [[nodiscard]] size_t tellg() const {
        return pos;
    }

    [[nodiscard]] size_t tellp() const {
        return pos;
    }

    void seek(size_t to) {
        pos = to;
    }

    void clear() {
        pos = 0;
        buf.clear();
    }

    bool good() const {
        return pos <= buf.size();
    }
    bool fail() const {
        return !good();
    }
};
#endif //VECTOR_STREAM_H
