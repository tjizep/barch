#include "resp_reply.h"

#include <charconv>
#include <cstring>
#include <limits>

#include <fast_float/fast_float.h>

namespace barch::resp {

namespace {

/** a RESP line is short; one this long without its CRLF is not a RESP line */
constexpr size_t max_line = 64 * 1024;

bool parse_int(const std::string& s, int64_t& out) {
    if (s.empty())
        return false;
    auto r = std::from_chars(s.data(), s.data() + s.size(), out);
    return r.ec == std::errc() && r.ptr == s.data() + s.size();
}

bool parse_double(const std::string& s, double& out) {
    if (s == "inf" || s == "+inf") {
        out = std::numeric_limits<double>::infinity();
        return true;
    }
    if (s == "-inf") {
        out = -std::numeric_limits<double>::infinity();
        return true;
    }
    if (s == "nan") {
        out = std::numeric_limits<double>::quiet_NaN();
        return true;
    }
    if (s.empty())
        return false;
    // fast_float rather than strtod: strtod follows the locale's decimal point
    auto r = fast_float::from_chars(s.data(), s.data() + s.size(), out);
    return r.ec == std::errc() && r.ptr == s.data() + s.size();
}

bool is_big_number(const std::string& s) {
    size_t i = (!s.empty() && (s[0] == '-' || s[0] == '+')) ? 1 : 0;
    if (i == s.size())
        return false;
    for (; i < s.size(); ++i) {
        if (s[i] < '0' || s[i] > '9')
            return false;
    }
    return true;
}

/** barch marks a bulk string with a leading `$`; every string gets it, so a simple
 *  string that happens to start with `$` can't be mistaken for one and stripped */
std::string marked(const char* p, size_t n) {
    std::string s;
    s.reserve(n + 1);
    s.push_back('$');
    s.append(p, n);
    return s;
}

}

void reply_parser::feed(const char* data, size_t n) {
    buf.append(data, n);
}

void reply_parser::compact() {
    if (pos == buf.size()) {
        buf.clear();
        pos = 0;
    } else if (pos > 1024 * 1024) {
        buf.erase(0, pos);
        pos = 0;
    }
}

Variable reply_parser::close(frame& f) const {
    switch (f.type) {
        case '%': {
            map_t m;
            m.items = std::move(f.items);
            return Variable(m);
        }
        case '~': {
            set_t s;
            s.items = std::move(f.items);
            return Variable(s);
        }
        default: // '*', and a '>' push that turned up inside something
            return Variable(std::move(f.items));
    }
}

bool reply_parser::deliver(Variable&& v, Variable& out) {
    if (stack.empty()) {
        out = std::move(v);
        reply_bytes = 0;
        return true;
    }
    auto& top = stack.back();
    top.items.emplace_back(static_cast<const variable_t&>(v));
    --top.remaining;
    return false;
}

int reply_parser::scalar_or_open(Variable& v, bool& opened, std::string& err) {
    opened = false;
    const size_t at = pos;
    const char type = buf[at];
    auto cr = buf.find("\r\n", at + 1);
    if (cr == std::string::npos) {
        if (buf.size() - at > max_line) {
            err = "a RESP line with no end";
            return -1;
        }
        return 0;
    }
    if (cr - at > max_line) {
        err = "a RESP line with no end";
        return -1;
    }
    std::string text = buf.substr(at + 1, cr - at - 1);
    const size_t after = cr + 2;

    switch (type) {
        case '+':
            v = Variable(marked(text.data(), text.size()));
            pos = after;
            return 1;
        case '-':
            v = Variable(error(text));
            pos = after;
            return 1;
        case ':': {
            int64_t n = 0;
            if (!parse_int(text, n)) {
                err = "a bad RESP integer";
                return -1;
            }
            v = Variable(n);
            pos = after;
            return 1;
        }
        case '(':
            // a RESP3 big number doesn't fit any Luau number, so it stays text
            if (!is_big_number(text)) {
                err = "a bad RESP big number";
                return -1;
            }
            v = Variable(marked(text.data(), text.size()));
            pos = after;
            return 1;
        case ',': {
            double d = 0;
            if (!parse_double(text, d)) {
                err = "a bad RESP double";
                return -1;
            }
            v = Variable(d);
            pos = after;
            return 1;
        }
        case '#':
            if (text != "t" && text != "f") {
                err = "a bad RESP boolean";
                return -1;
            }
            v = Variable(text == "t");
            pos = after;
            return 1;
        case '_':
            if (!text.empty()) {
                err = "a bad RESP null";
                return -1;
            }
            v = Variable(nullptr);
            pos = after;
            return 1;
        case '$':
        case '!':
        case '=': {
            int64_t len = 0;
            if (!parse_int(text, len)) {
                err = "a bad RESP length";
                return -1;
            }
            if (type == '$' && len == -1) {
                v = Variable(nullptr);          // RESP2's null bulk string
                pos = after;
                return 1;
            }
            if (len < 0) {
                err = "a bad RESP length";
                return -1;
            }
            if ((size_t) len > lim.max_bulk) {
                err = "a RESP string larger than the limit";
                return -1;
            }
            if (buf.size() < after + (size_t) len + 2)
                return 0;                       // the header stays unread until it all arrives
            if (buf[after + len] != '\r' || buf[after + len + 1] != '\n') {
                err = "a RESP string without its CRLF";
                return -1;
            }
            const char* data = buf.data() + after;
            if (type == '$') {
                v = Variable(marked(data, (size_t) len));
            } else if (type == '!') {
                v = Variable(error(std::string(data, (size_t) len)));
            } else {
                // `=` is `fmt:text`, the format three letters
                if (len < 4 || data[3] != ':') {
                    err = "a bad RESP verbatim string";
                    return -1;
                }
                verbatim_t vb;
                vb.format.assign(data, 3);
                vb.text.assign(data + 4, (size_t) len - 4);
                v = Variable(vb);
            }
            pos = after + (size_t) len + 2;
            return 1;
        }
        case '*':
        case '%':
        case '~':
        case '>':
        case '|': {
            int64_t count = 0;
            if (!parse_int(text, count)) {
                err = "a bad RESP length";
                return -1;
            }
            if (type == '*' && count == -1) {
                v = Variable(nullptr);          // RESP2's null array
                pos = after;
                return 1;
            }
            if (count < 0) {
                err = "a bad RESP length";
                return -1;
            }
            // a map and an attribute count pairs; everything else counts elements
            uint64_t n = (type == '%' || type == '|') ? (uint64_t) count * 2 : (uint64_t) count;
            if (n > lim.max_elements) {
                err = "a RESP aggregate larger than the limit";
                return -1;
            }
            if ((int) stack.size() + 1 > lim.max_depth) {
                err = "RESP nested deeper than the limit";
                return -1;
            }
            frame f;
            f.type = type;
            f.remaining = (size_t) n;
            // not reserved to n: the length is the server's word, and reserving on it
            // would let a stream that never delivers allocate the whole limit up front
            stack.push_back(std::move(f));
            opened = true;
            pos = after;
            return 1;
        }
        default:
            err = std::string("not a RESP reply (first byte ") + std::to_string((unsigned char) type) + ")";
            return -1;
    }
}

int reply_parser::next(Variable& out, std::string& err) {
    if (broken) {
        err = "the RESP stream already failed";
        return -1;
    }
    for (;;) {
        // close whatever this has just completed, innermost first
        while (!stack.empty() && stack.back().remaining == 0) {
            frame f = std::move(stack.back());
            stack.pop_back();
            if (f.type == '|')
                continue;       // an attribute describes the value after it; that is what's wanted
            if (f.type == '>' && stack.empty()) {
                reply_bytes = 0; // an out of band push, not the reply to anything asked
                continue;
            }
            if (deliver(close(f), out))
                return 1;
        }
        if (pos >= buf.size()) {
            compact();
            return 0;
        }
        Variable v;
        bool opened = false;
        const size_t before = pos;
        int r = scalar_or_open(v, opened, err);
        if (r == 0) {
            compact();
            return 0;
        }
        if (r < 0) {
            broken = true;
            return -1;
        }
        reply_bytes += pos - before;
        if (reply_bytes > lim.max_reply) {
            err = "a RESP reply larger than the limit";
            broken = true;
            return -1;
        }
        if (opened)
            continue;
        if (deliver(std::move(v), out))
            return 1;
    }
}

}
