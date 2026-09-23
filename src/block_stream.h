#pragma once
//
// Streams in blocks - TODO 418. A streaming save writes a shard through an
// ostream whose bytes leave in fixed size blocks, numbered from 0 per shard,
// and a streaming load reads a shard back out of memory it has collected.
// Neither side seeks: the format carries a trailer instead of the stamp the
// file save goes back to write.
//
#include <cstdint>
#include <functional>
#include <streambuf>
#include <vector>

namespace barch {

/** the size a streamed block is cut at; the last one of a shard is shorter */
constexpr size_t stream_block_size = 65536;

/**
 * An ostream's buffer that hands each full block to `emit(data, len, block)`,
 * and the partial last one on sync. `emit` answering false stops the stream:
 * the ostream goes bad and every write after it fails.
 */
class block_out : public std::streambuf {
public:
    using emit_fn = std::function<bool(const char* data, size_t len, uint64_t block)>;
    explicit block_out(emit_fn emit, size_t size = stream_block_size)
        : emit(std::move(emit)), buf(size) {
        setp(buf.data(), buf.data() + buf.size());
    }
    /** blocks handed over so far */
    uint64_t blocks() const { return next; }

protected:
    int overflow(int c) override {
        if (!flush_block())
            return traits_type::eof();
        if (c != traits_type::eof()) {
            *pptr() = (char) c;
            pbump(1);
        }
        return traits_type::not_eof(c);
    }
    int sync() override {
        return flush_block() ? 0 : -1;
    }

private:
    bool flush_block() {
        if (stopped)
            return false;
        size_t n = (size_t) (pptr() - pbase());
        if (n == 0)
            return true;
        if (!emit(pbase(), n, next)) {
            stopped = true;
            return false;
        }
        ++next;
        setp(buf.data(), buf.data() + buf.size());
        return true;
    }
    emit_fn emit;
    std::vector<char> buf;
    uint64_t next{0};
    bool stopped{false};
};

/** an istream's buffer over bytes already in memory, without copying them */
class memory_in : public std::streambuf {
public:
    memory_in(const char* data, size_t len) {
        char* p = const_cast<char*>(data);
        setg(p, p, p + len);
    }
};

}
