//
// Bounds of the glob bracket matcher.
//
// A pattern arrives as `art::value_type`: a pointer and a length, with no byte
// after the end that the matcher is allowed to read. The bracket case used to
// look at `pattern[0]` before it checked `patternLen == 0`, so a pattern ending
// in an unterminated `[` read one byte past its end. This runs such patterns out
// of buffers sized to exactly the pattern, so the read lands in an AddressSanitizer
// redzone and aborts. See TODO 449.
//
#include "value_type.h"
#include "glob.h"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace {
    int failures = 0;

    void check(bool ok, const char* what) {
        if (!ok) {
            std::printf("FAIL: %s\n", what);
            ++failures;
        }
    }

    int match(const std::string& pattern, const std::string& subject, int nocase = 0) {
        return glob::stringmatchlen(art::value_type(pattern.data(), pattern.size()),
                                    art::value_type(subject.data(), subject.size()), nocase);
    }

    // Run a pattern out of a buffer sized to exactly the pattern, and require the
    // same answer as the same pattern with a NUL after it - which is what the
    // matcher would see if the pattern were a C string. Pre-fix the exact-sized
    // read goes into the redzone and AddressSanitizer aborts; even without one it
    // would be reading whatever the next byte happens to be.
    void exact(const std::vector<std::string>& patterns, const std::string& subject) {
        for (const auto& p : patterns) {
            std::string terminated = p;
            terminated.push_back('\0');
            int want = glob::stringmatchlen(
                art::value_type(terminated.data(), (unsigned) p.size()),
                art::value_type(subject.data(), subject.size()), 0);

            char* buf = new char[p.size()];
            std::memcpy(buf, p.data(), p.size());
            int got = glob::stringmatchlen(art::value_type(buf, (unsigned) p.size()),
                                           art::value_type(subject.data(), subject.size()), 0);
            delete[] buf;
            check(got == want, "an exact-sized unterminated bracket agrees with a terminated one");
        }
    }
}

int main() {
    // ordinary bracket behaviour is unchanged
    check(match("[abc]", "a") == 1, "bracket class");
    check(match("[abc]", "d") == 0, "bracket class miss");
    check(match("[a-c]", "b") == 1, "bracket range");
    check(match("[a-c]", "z") == 0, "bracket range miss");
    check(match("[^abc]", "d") == 1, "negated class");
    check(match("[^abc]", "a") == 0, "negated class miss");
    check(match("[a-c][x-z]", "bz") == 1, "two classes");
    check(match("a[", "ab") == 0, "an unterminated bracket after a literal matches nothing");
    check(match("[", "a") == 0, "a lone unterminated bracket matches nothing");

    // the same unterminated shapes, but with no readable byte past the end
    exact({"[", "[abc", "[^", "[]", "[abc-", "[a-"}, "a");
    exact({"a[", "ab[", "a[bc"}, "ab");
    exact({"*["}, "abc");

    if (failures) {
        std::printf("%d check(s) failed\n", failures);
        return 1;
    }
    std::printf("glob bracket bounds: ok\n");
    return 0;
}
