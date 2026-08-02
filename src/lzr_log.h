//
// Created by teejip on 8/2/26.
//

#ifndef LZR_LOG_H
#define LZR_LOG_H
#include <cstdint>
#include <initializer_list>
#include <string>
#include <string_view>

/*
 * Logging that costs almost nothing to include or to call.
 *
 *     log({"loaded", count, "shards in", seconds, "s"});
 *     warn({"timeout after", secs, "seconds"});
 *     err({"could not save shard", shard_num});
 *
 * Two things are deliberate about this header.
 *
 * It is not a template. logger.h's std_log(a, b, c) is variadic, so every distinct
 * combination of argument types instantiates another function and another
 * fmt::make_format_args. These take one initializer_list and instantiate nothing.
 *
 * It includes almost nothing. Not fmt, which logger.h drags in along with its chrono
 * and color headers, and not variable.h, which Variable would have required and which
 * is about a second of compile time on its own. <string> is here only so that a
 * std::string can be logged directly: it reaches string_view through a user defined
 * conversion, and a second one to log_value is more than an implicit sequence will do,
 * so it needs a constructor of its own. Any translation unit holding a std::string to
 * log has included <string> already. log_value carries only what a log line
 * can contain and is built entirely from non-template constructors, so a call site
 * generates no template code at all - which is what the variant's converting
 * assignment would otherwise have done for every argument.
 *
 * All the formatting lives in lzr_log.cpp, the only translation unit that needs fmt.
 */
namespace barch {
    /**
     * One piece of a log line. Deliberately narrow: the kinds a message is made of and
     * nothing else, which is the difference between this and Variable.
     *
     * Text is borrowed, not copied. That is safe for the only thing this is for - the
     * list lives for the duration of the call and the line is written before it
     * returns - and it means logging a string costs nothing to pass.
     */
    class log_value {
    public:
        enum kind : unsigned char { text, character, boolean, integer, unsigned_integer, real };

        log_value(bool v) : k(boolean) { as.b = v; }
        log_value(char v) : k(character) { as.c = v; }
        log_value(signed char v) : k(integer) { as.i = v; }
        log_value(short v) : k(integer) { as.i = v; }
        log_value(int v) : k(integer) { as.i = v; }
        log_value(long v) : k(integer) { as.i = v; }
        log_value(long long v) : k(integer) { as.i = v; }
        log_value(unsigned char v) : k(unsigned_integer) { as.u = v; }
        log_value(unsigned short v) : k(unsigned_integer) { as.u = v; }
        log_value(unsigned int v) : k(unsigned_integer) { as.u = v; }
        log_value(unsigned long v) : k(unsigned_integer) { as.u = v; }
        log_value(unsigned long long v) : k(unsigned_integer) { as.u = v; }
        log_value(float v) : k(real) { as.d = v; }
        log_value(double v) : k(real) { as.d = v; }
        // art::value_type is not named here and does not need to be: it has to_view(),
        // and writing that at the call site keeps this header free of its include
        log_value(std::string_view v) : k(text), str(v) {}
        log_value(const std::string& v) : k(text), str(v) {}
        log_value(const char* v) : k(text), str(v ? std::string_view(v) : std::string_view()) {}
        // a non const char* is an exact match for the deleted template below, where
        // const char* would only reach its constructor by a qualification conversion,
        // so it has to be named too
        log_value(char* v) : k(text), str(v ? std::string_view(v) : std::string_view()) {}

        // without this a stray pointer would reach log_value(bool) through the pointer
        // to bool conversion and quietly log "true". const char* still binds to its own
        // constructor above, which is not a template and so wins
        template<typename T> log_value(T*) = delete;
        log_value(std::nullptr_t) = delete;

        kind k;
        std::string_view str{};
        union scalar {
            bool b;
            char c;
            int64_t i;
            uint64_t u;
            double d;
        } as{};
    };

    /**
     * The pieces of one line, written in order and separated by spaces - the same shape
     * the old logger produced, trailing space included, so moving a call site across
     * does not move its output.
     */
    typedef std::initializer_list<log_value> log_line;

    /** an ordinary message */
    void log(log_line parts);
    /** something worth noticing that is not yet a failure */
    void warn(log_line parts);
    /** a failure */
    void err(log_line parts);
}

#endif //LZR_LOG_H
