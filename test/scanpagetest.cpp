// Container scan pages under every short interleaving of changes - TODO 951.
//
// src/scan_page_core.h ends a page after COUNT live entries and saves the first one
// it does not take; the next page starts at that key, inclusive. This runs a model of
// ZSCAN: four members, a walk over their keys in order, and one change between pages.
// It starts from every order of the scores {1, 2, 3, 4}, at COUNT 1 and 2, and tries
// every sequence of up to three changes, one after each page: nothing, remove a member,
// add one back, or set a member's score to -1, 0 or 5. It checks:
//   - every member there throughout the iteration is reported;
//   - nothing is reported that was never there;
//   - the iteration ends within 50 pages.
//
// It also runs rules that are known to be wrong and checks that it catches each: a walk
// in score order, which loses a member whose score drops behind the saved key; a next
// page that starts after the saved key; and a page that never takes an entry.
#include "scan_page_core.h"

#include <algorithm>
#include <cstdio>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

using namespace barch::scan_page_core;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    using state = std::map<char, int>;          // member -> score
    using key = std::pair<int, char>;           // a walk key: (order value, member)

    struct rules {
        std::function<key(char, int)> walk_key;  // where a member sits in the walk
        bool inclusive;
        std::function<step(page&)> at;
    };

    const rules real{
        [](char m, int) { return key{0, m}; },  // the member index: by name only
        resume_inclusive,
        [](page& p) { return p.at(); },
    };

    struct change {
        enum { none, remove, add, rescore } what;
        char m;
        int score;
    };

    std::vector<change> changes() {
        std::vector<change> out{{change::none, 0, 0}};
        for (char m : {'a', 'b', 'c', 'd'}) {
            out.push_back({change::remove, m, 0});
            out.push_back({change::add, m, 3});
            for (int s : {-1, 0, 5}) out.push_back({change::rescore, m, s});
        }
        return out;
    }

    const std::vector<change> all_changes = changes();

    void apply(state& s, const change& c, std::set<char>& ever, std::set<char>& gone) {
        switch (c.what) {
            case change::none:
                break;
            case change::remove:
                if (s.erase(c.m)) gone.insert(c.m);
                break;
            case change::add:
                if (!s.count(c.m)) {
                    s[c.m] = c.score;
                    ever.insert(c.m);
                    gone.insert(c.m); // not there throughout
                }
                break;
            case change::rescore:
                if (s.count(c.m)) s[c.m] = c.score;
                break;
        }
    }

    /** one page: what it reports, and the saved key, or none when it is complete */
    std::optional<key> one_page(const rules& r, const state& s, long long count,
                                const std::optional<key>& from, std::set<char>& reported) {
        std::vector<key> keys;
        for (auto& [m, sc] : s) keys.push_back(r.walk_key(m, sc));
        std::sort(keys.begin(), keys.end());
        auto it = keys.begin();
        if (from) {
            it = r.inclusive ? std::lower_bound(keys.begin(), keys.end(), *from)
                             : std::upper_bound(keys.begin(), keys.end(), *from);
        }
        page p(count);
        for (; it != keys.end(); ++it) {
            if (r.at(p) == step::resume) return *it;
            reported.insert(it->second);
        }
        return std::nullopt;
    }

    long long runs = 0;

    /** the number of iterations the rules get wrong, over every sequence of changes */
    int run(const rules& r, const state& start, long long count, std::vector<change>& seq) {
        int bad = 0;
        // this sequence as it stands, with no more changes
        {
            ++runs;
            state s = start;
            std::set<char> ever, gone, reported;
            for (auto& [m, sc] : s) ever.insert(m);
            std::optional<key> cursor;
            size_t pages = 0;
            bool ended = false;
            while (pages < 50) {
                cursor = one_page(r, s, count, cursor, reported);
                ++pages;
                if (!cursor) {
                    ended = true;
                    break;
                }
                if (pages <= seq.size()) apply(s, seq[pages - 1], ever, gone);
            }
            bool ok = ended;
            for (auto& [m, sc] : start)
                if (!gone.count(m) && !reported.count(m)) ok = false;
            for (char m : reported)
                if (!ever.count(m)) ok = false;
            if (!ok) ++bad;
        }
        if (seq.size() == 3) return bad;
        for (const change& c : all_changes) {
            seq.push_back(c);
            bad += run(r, start, count, seq);
            seq.pop_back();
        }
        return bad;
    }

    int wrong(const rules& r) {
        int bad = 0;
        std::vector<int> scores{1, 2, 3, 4};
        do {
            state start{{'a', scores[0]}, {'b', scores[1]}, {'c', scores[2]}, {'d', scores[3]}};
            for (long long count : {1, 2}) {
                std::vector<change> seq;
                bad += run(r, start, count, seq);
            }
        } while (std::next_permutation(scores.begin(), scores.end()));
        return bad;
    }
}

int main() {
    check(wrong(real) == 0, "every member there throughout is reported, every interleaving");
    std::printf("  %lld iterations\n", runs);

    page p(2);
    check(p.at() == step::take && p.at() == step::take && p.at() == step::resume,
          "a page of 2 takes two and resumes at the third");

    rules m = real;
    m.walk_key = [](char member, int score) { return key{score, member}; };
    check(wrong(m) > 0, "mutant: a walk in score order is caught");

    m = real;
    m.inclusive = false;
    check(wrong(m) > 0, "mutant: a next page that starts after the saved key is caught");

    m = real;
    m.at = [](page&) { return step::resume; };
    check(wrong(m) > 0, "mutant: a page that never takes is caught");

    std::printf("%s\n", failures ? "FAILED" : "all passed");
    return failures ? 1 : 0;
}
