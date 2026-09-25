//
// A null bulk string in a request.
//
// RESP carries a null bulk as `$-1\r\n`. The request parser used to replace it
// with the literal text "NULL", so a null key silently addressed the key named
// NULL and a null value stored the word. A command cannot be given a null, so it
// is now refused. See TODO 449.
//
#include "rpc/redis_parser.h"

#include <cstdio>
#include <cstring>
#include <exception>
#include <string>

namespace {
    int failures = 0;

    void check(bool ok, const char* what) {
        if (!ok) {
            std::printf("FAIL: %s\n", what);
            ++failures;
        }
    }

    bool refused(const char* request) {
        redis::redis_parser parser;
        try {
            parser.add_data(request, std::strlen(request));
            parser.read_new_request();
        } catch (const std::exception&) {
            return true;
        }
        return false;
    }
}

int main() {
    // a well formed request still parses, so the refusal is not simply "nothing works"
    {
        redis::redis_parser parser;
        const char* request = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n";
        parser.add_data(request, std::strlen(request));
        const auto& r = parser.read_new_request();
        check(r.size() == 3, "a well formed request parses");
        if (r.size() == 3) {
            check(std::string(r[0]) == "SET", "command");
            check(std::string(r[1]) == "k", "key");
            check(std::string(r[2]) == "hello", "value");
        }
    }

    // a null bulk key
    check(refused("*2\r\n$-1\r\n$1\r\nv\r\n"),
          "a null bulk key is refused, not read as the text NULL");

    // a null bulk value
    check(refused("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$-1\r\n"),
          "a null bulk value is refused, not stored as the text NULL");

    // a null bulk in a longer request, after a good argument
    check(refused("*4\r\n$4\r\nZADD\r\n$1\r\nz\r\n$-1\r\n$1\r\nm\r\n"),
          "a null bulk among good arguments is refused");

    if (failures) {
        std::printf("%d check(s) failed\n", failures);
        return 1;
    }
    std::printf("resp null bulk: ok\n");
    return 0;
}
