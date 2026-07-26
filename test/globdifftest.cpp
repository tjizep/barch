//
// Differential test for the glob matcher in src/glob.cpp.
//
// glob::stringmatchlen picks one of two implementations. A pattern built only from
// literals, '*', '?' and '\' goes to the optimised asterisk_impl; the moment a '['
// shows up it falls back to stringmatchlen_impl, which is the untouched redis
// matcher. The two must always agree, so this walks a generated corpus of patterns
// and subjects and asks both.
//
// Any disagreement reported here is a bug in the optimised path - the reference
// side is the same code redis ships.
//

// the heavy headers go in first, then glob's private section is reopened so the
// reference matcher can be called directly. access specifiers do not change the
// mangled name, so this still links against the real glob.cpp
#include "value_type.h"
#define private public
#include "glob.h"
#undef private

#include <cctype>
#include <cstdio>
#include <string>
#include <vector>

namespace {

struct failure {
    std::string pattern;
    std::string subject;
    int nocase;
    int expected;
    int got;
};

std::vector<failure> samples; // a bounded sample kept for the report
long long failed = 0;
long long checked = 0;

int reference(const std::string &pat, const std::string &str, int nocase) {
    int skip = 0;
    return glob::stringmatchlen_impl(pat.data(), (int) pat.size(),
                                     str.data(), (int) str.size(), nocase, &skip, 0);
}

int optimised(const std::string &pat, const std::string &str, int nocase) {
    return glob::stringmatchlen(art::value_type(pat.data(), pat.size()),
                                art::value_type(str.data(), str.size()), nocase);
}

void check(const std::string &pat, const std::string &str, int nocase) {
    ++checked;
    int want = reference(pat, str, nocase);
    int got = optimised(pat, str, nocase);
    if (want == got) return;
    ++failed;
    if (samples.size() < 4000) samples.push_back({pat, str, nocase, want, got});
}

// every string over the alphabet up to maxLen, the empty one included
void expand(const std::string &alphabet, size_t maxLen, std::vector<std::string> &out) {
    out.emplace_back("");
    size_t start = 0;
    for (size_t len = 1; len <= maxLen; ++len) {
        size_t end = out.size();
        for (size_t i = start; i < end; ++i) {
            for (char c : alphabet) out.push_back(out[i] + c);
        }
        start = end;
    }
}

std::string show(const std::string &s) {
    std::string r = "\"";
    for (char c : s) {
        if (c == '\\') r += "\\\\";
        else r += c;
    }
    return r + "\"";
}

std::string upper(const std::string &s) {
    std::string r = s;
    for (auto &c : r) c = (char) std::toupper((unsigned char) c);
    return r;
}

void report_group(const char *title, bool nocase_wanted, bool with_star_question) {
    std::vector<std::string> seen;
    int shown = 0;
    for (const auto &f : samples) {
        bool has = f.pattern.find("*?") != std::string::npos;
        if (has != with_star_question) continue;
        if ((f.nocase != 0) != nocase_wanted) continue;
        bool dup = false;
        for (const auto &s : seen) if (s == f.pattern) { dup = true; break; }
        if (dup) continue;
        seen.push_back(f.pattern);
        if (shown++ >= 12) continue;
        printf("    pattern %-12s subject %-10s reference=%d optimised=%d\n",
               show(f.pattern).c_str(), show(f.subject).c_str(), f.expected, f.got);
    }
    if (!seen.empty()) {
        printf("  %s: %zu distinct patterns in the sample\n\n", title, seen.size());
    }
}

} // namespace

int main() {
    // stage 1 - every construct the optimised path can see, against a two letter
    // subject alphabet. small alphabets are what make the shortcuts collide
    std::vector<std::string> patterns, subjects;
    expand("ab*?\\", 6, patterns);
    expand("ab", 5, subjects);
    for (const auto &p : patterns)
        for (const auto &s : subjects)
            check(p, s, 0);
    long long after1 = failed, count1 = checked;

    // stage 2 - a wider alphabet so a literal run after a star can miss in more ways
    std::vector<std::string> patterns2, subjects2;
    expand("abc*?", 5, patterns2);
    expand("abc", 4, subjects2);
    for (const auto &p : patterns2)
        for (const auto &s : subjects2)
            check(p, s, 0);
    long long after2 = failed, count2 = checked;

    // stage 3 - the same corpus with case folding on. the shortcuts lean on
    // memmem and memchr, which compare bytes, so this is worth its own sweep
    for (const auto &p : patterns2)
        for (const auto &s : subjects2)
            check(p, upper(s), 1);
    long long after3 = failed, count3 = checked;

    // stage 4 - hand picked shapes that sit right on the shortcut thresholds
    static const char *pairs[][2] = {
        {"*abcd", "abcd"}, {"*abcd", "xabcd"}, {"*abcd", "abc"},
        {"*?abcd", "abcd"}, {"*??abcd", "abcd"}, {"*???abcd", "abcd"},
        {"*????", "abc"}, {"*?????", "abcd"}, {"*??", "a"},
        {"*ab*cd", "abxcd"}, {"a*bcde", "abcde"}, {"*abcd*efgh", "abcdefgh"},
        {"*\\*abc", "x*abc"}, {"*a\\bcd", "xabcd"},
        {"*aabbb", "aXaabbb"}, {"*mississippi", "mississippi"},
        {"*bananana", "bananana"}, {"*anana", "banana"},
    };
    for (auto &pr : pairs) {
        check(pr[0], pr[1], 0);
        check(pr[0], upper(pr[1]), 1);
        check(upper(pr[0]), pr[1], 1);
    }

    // stage 5 - patterns that are not NUL terminated. both matchers used to read one
    // byte past the end when collapsing a run of stars, so a hostile trailing byte
    // changed the answer. a differential cannot see this because both sides shared the
    // bug, so these carry the expected result instead.
    long long unterminated_failures = 0;
    {
        struct probe {
            const char *buffer;   // what sits in memory
            int len;              // how much of it is the pattern
            const char *subject;
            int expected;
        };
        static const probe probes[] = {
            // pattern "*" with a stray '*' sitting just past the end
            {"**",    1, "abc", 1},
            // an empty subject matches nothing but an empty pattern - the main loop
            // needs a character to run at all - so '*' against "" is 0 in stock redis
            // too, and both matchers have to agree on that
            {"**",    1, "",    0},
            // pattern "a*" where the star run collapse would walk off the end
            {"a**",   2, "abc", 1},
            {"*a**",  3, "xa",  1},
            // pattern "a" spent exactly as the subject runs out, stray '*' after it
            {"a*",    1, "a",   1},
            {"ab*",   2, "ab",  1},
            // and the same shapes where the answer is a miss
            {"a*",    1, "b",   0},
            {"ab*",   2, "ac",  0},
        };
        for (const auto &p : probes) {
            std::string subject = p.subject;
            for (int nocase = 0; nocase < 2; ++nocase) {
                int got_opt = glob::stringmatchlen(art::value_type(p.buffer, (size_t) p.len),
                                                   art::value_type(subject.data(), subject.size()), nocase);
                int skip = 0;
                int got_ref = glob::stringmatchlen_impl(p.buffer, p.len, subject.data(),
                                                        (int) subject.size(), nocase, &skip, 0);
                if (got_opt != p.expected || got_ref != p.expected) {
                    ++unterminated_failures;
                    printf("  buffer %-8s used as a %d byte pattern against %-6s nocase=%d "
                           "expected=%d optimised=%d reference=%d\n",
                           show(p.buffer).c_str(), p.len, show(subject).c_str(), nocase,
                           p.expected, got_opt, got_ref);
                }
                ++checked;
            }
        }

        // the probes above sit inside a string literal, so the byte past the pattern is
        // still owned memory and a sanitizer sees nothing - it is the answer that gives
        // the bug away. run the same shapes once more out of an exactly sized heap
        // allocation, where reading one byte too far lands in a redzone. the result is
        // whatever the neighbouring byte says, so nothing is asserted about it here;
        // this pass exists purely so a sanitizer build has something to catch.
        for (const auto &p : probes) {
            char *exact = new char[p.len];
            memcpy(exact, p.buffer, p.len);
            std::string subject = p.subject;
            glob::stringmatchlen(art::value_type(exact, (size_t) p.len),
                                 art::value_type(subject.data(), subject.size()), 0);
            int skip = 0;
            glob::stringmatchlen_impl(exact, p.len, subject.data(), (int) subject.size(), 0, &skip, 0);
            delete[] exact;
        }
    }

    printf("glob differential: optimised asterisk_impl against the reference matcher\n\n");
    printf("  stage 1  literals/*/?/backslash x ab   %9lld checked %8lld disagreements\n",
           count1, after1);
    printf("  stage 2  literals/*/? x abc            %9lld checked %8lld disagreements\n",
           count2 - count1, after2 - after1);
    printf("  stage 3  stage 2 corpus, nocase        %9lld checked %8lld disagreements\n",
           count3 - count2, after3 - after2);
    printf("  stage 4  hand picked shapes            %9lld checked %8lld disagreements\n",
           checked - count3, failed - after3);
    printf("  stage 5  unterminated pattern buffers  %9d checked %8lld wrong answers\n",
           16, unterminated_failures);
    printf("  total                                  %9lld checked %8lld disagreements\n\n",
           checked, failed);

    if (unterminated_failures) {
        printf("FAILED: a pattern that is not NUL terminated is read past its end\n");
        return 1;
    }
    if (failed) {
        printf("  a star immediately followed by a question mark, case sensitive:\n");
        report_group("star-question", false, true);
        printf("  a star immediately followed by a question mark, case insensitive:\n");
        report_group("star-question nocase", true, true);
        printf("  everything else, case sensitive:\n");
        report_group("other", false, false);
        printf("  everything else, case insensitive:\n");
        report_group("other nocase", true, false);
        printf("FAILED: the optimised glob path does not agree with the reference matcher\n");
        return 1;
    }

    printf("OK: the two matchers agree on every case\n");
    return 0;
}