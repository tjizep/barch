#pragma once
//
// Parsing RESP replies - what a server sends back, as opposed to redis_parser, which
// reads the requests a client sends barch. TODO 379.
//
// Incremental: bytes go in as they arrive and `next` hands out one complete reply at a
// time. A reply split across reads picks up where it stopped - the arrays and maps it
// was in the middle of are kept on a stack - rather than being parsed again from the
// start, so a big reply arriving in small pieces costs time in proportion to its size.
//
// Replies come out as Variables with the same shapes barch.call produces: every string
// carries the `$` marker barch uses for a bulk string (`push_variable` strips it), a
// RESP3 map is a flat list of keys and values, an error is a var_error.
//
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "variable.h"

namespace barch::resp {

struct limits {
    /** the largest single string or blob a reply may carry */
    size_t max_bulk{64u * 1024u * 1024u};
    /** the most bytes one whole reply may take on the wire */
    size_t max_reply{256u * 1024u * 1024u};
    /** the most elements in one array, set or map (a map counts keys and values) */
    size_t max_elements{16u * 1024u * 1024u};
    /** how deeply aggregates may nest */
    int max_depth{32};
};

class reply_parser {
public:
    explicit reply_parser(limits l = {}) : lim(l) {}

    /** bytes as they came off the socket */
    void feed(const char* data, size_t n);

    /**
     * The limits for the replies from here on. A pooled connection outlives the
     * settings it was opened with, so each exchange sets its own - only between
     * replies, while idle() is true.
     */
    void set_limits(const limits& l) { lim = l; }

    /**
     * One complete reply into `out`.
     * @return 1 for a reply, 0 when more bytes are needed, -1 when the stream is not
     * RESP or breaks a limit - `err` says which. After -1 the stream has lost its
     * place and the connection must not be used again.
     */
    int next(Variable& out, std::string& err);

    /** bytes received and not yet part of a reply handed out */
    [[nodiscard]] size_t buffered() const { return buf.size() - pos; }

    /** true between replies: nothing half parsed */
    [[nodiscard]] bool idle() const { return stack.empty() && buffered() == 0; }

private:
    struct frame {
        char type{0};
        size_t remaining{0};
        heap::vector<wrapped_variable_t> items{};
    };

    // 1 a value, 0 need more (pos untouched), -1 error
    int scalar_or_open(Variable& v, bool& opened, std::string& err);
    Variable close(frame& f) const;
    // true when the reply is complete and in `out`
    bool deliver(Variable&& v, Variable& out);
    void compact();

    limits lim;
    std::string buf;
    size_t pos{0};
    // bytes of the reply being built, counted as they are consumed so compacting the
    // buffer doesn't reset it - max_reply covers a reply spread over many reads
    size_t reply_bytes{0};
    std::vector<frame> stack;
    bool broken{false};
};

}
