// Whether a score change moves a member's key, over every short run of increments - TODO 950.
//
// src/score_move_core.h decides whether ZINCRBY replaces a member's score key or keeps
// it. This runs a model of the sorted set: a score-ordered map of keys, and a member
// index that points at each member's key. ZINCRBY there does what ordered_api.cpp
// does: it writes the new key, points the index at it, then removes the old key,
// unless the core says keep. It starts two members at every pair of scores in
// {-inf, -1, -0, 0, 1, 1e300, inf} and runs every sequence of up to three increments
// of {0, -0, 1e-300, 1, -1, inf} on either member. After each step it checks:
//   - both members are still in the score map, and nothing else is;
//   - the index points at a key that is there;
//   - the key encodes exactly the score ZINCRBY answered.
// An increment that would make a nan is refused and changes nothing.
//
// It also runs rules that are known to be wrong and checks that it catches each: always
// replace, as before TODO 950; always keep; and keep when the doubles compare equal,
// which keeps -0's key for a score of 0.
#include "score_move_core.h"

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <string>
#include <vector>

using namespace barch::score_move_core;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    const double inf = std::numeric_limits<double>::infinity();

    /** an order keeping encoding that tells 0 and -0 apart, as the strictest key would */
    std::string enc(double d) {
        uint64_t u;
        std::memcpy(&u, &d, sizeof u);
        u = (u >> 63) ? ~u : u | (1ull << 63);
        std::string out(8, '\0');
        for (int i = 0; i < 8; ++i) out[i] = char(u >> (56 - 8 * i));
        return out;
    }

    double dec(const std::string& k) {
        uint64_t u = 0;
        for (int i = 0; i < 8; ++i) u = (u << 8) | uint8_t(k[i]);
        u = (u >> 63) ? u & ~(1ull << 63) : ~u;
        double d;
        std::memcpy(&d, &u, sizeof d);
        return d;
    }

    using rule = std::function<step(const std::string& old_key, const std::string& new_key)>;

    struct zset {
        std::map<std::string, std::string> scores; // score key -> member
        std::map<std::string, std::string> index;  // member -> score key

        void add(const std::string& m, double s) {
            const auto k = enc(s) + m;
            scores[k] = m;
            index[m] = k;
        }

        /** false when refused as a nan; else the score it answers */
        bool incr(const rule& r, const std::string& m, double by, double& answer) {
            const auto old_key = index.at(m);
            const double s = dec(old_key) + by;
            if (std::isnan(s)) return false;
            const auto new_key = enc(s) + m;
            answer = s;
            if (r(old_key, new_key) == step::keep) return true;
            scores[new_key] = m;
            index[m] = new_key;
            scores.erase(old_key);
            return true;
        }

        bool sound(const std::string& m, const double* answer) const {
            if (scores.size() != index.size()) return false;
            auto i = index.find(m);
            if (i == index.end()) return false;
            auto s = scores.find(i->second);
            if (s == scores.end() || s->second != m) return false;
            return !answer || i->second == enc(*answer) + m;
        }
    };

    const double starts[] = {-inf, -1, -0.0, 0.0, 1, 1e300, inf};
    const double incrs[] = {0.0, -0.0, 1e-300, 1, -1, inf};
    const char* members[] = {"a", "b"};

    int steps = 0;

    /** runs every sequence of up to depth more increments from z; counts the bad steps */
    int walk(const rule& r, const zset& z, int depth) {
        if (depth == 0) return 0;
        int bad = 0;
        for (const char* m : members)
            for (double by : incrs) {
                zset next = z;
                if (next.index.size() != 2 || !next.sound(m, nullptr)) {
                    ++bad;
                    continue;
                }
                double answer = 0;
                const double before = dec(next.index.at(m));
                const bool done = next.incr(r, m, by, answer);
                ++steps;
                bool ok = next.sound("a", nullptr) && next.sound("b", nullptr);
                if (done) ok = ok && next.sound(m, &answer);
                else ok = ok && dec(next.index.at(m)) == before;
                if (!ok) {
                    ++bad;
                    continue;
                }
                bad += walk(r, next, depth - 1);
            }
        return bad;
    }

    int wrong(const rule& r) {
        int bad = 0;
        for (double sa : starts)
            for (double sb : starts) {
                zset z;
                z.add("a", sa);
                z.add("b", sb);
                bad += walk(r, z, 3);
            }
        return bad;
    }
}

int main() {
    const rule real = [](const std::string& o, const std::string& n) { return plan(o, n); };
    check(wrong(real) == 0, "every run of up to three increments keeps both members");
    std::printf("  %d steps\n", steps);

    zset z;
    z.add("m", 1);
    double answer = 0;
    z.incr(real, "m", 0.0, answer);
    check(z.scores.size() == 1 && answer == 1, "ZINCRBY by 0 keeps the member");
    z.incr(real, "m", 1e-300, answer);
    check(z.scores.size() == 1 && answer == 1, "ZINCRBY too small to change the double keeps it");
    z.incr(real, "m", 1, answer);
    check(z.scores.size() == 1 && z.sound("m", &answer) && answer == 2, "ZINCRBY by 1 moves it to 2");

    check(wrong([](const std::string&, const std::string&) { return step::replace; }) > 0,
          "mutant: always replace, as before TODO 950, is caught");
    check(wrong([](const std::string&, const std::string&) { return step::keep; }) > 0,
          "mutant: always keep is caught");
    check(wrong([](const std::string& o, const std::string& n) {
              return dec(o) == dec(n) ? step::keep : step::replace;
          }) > 0,
          "mutant: keep when the doubles compare equal is caught");

    std::printf("%s\n", failures ? "FAILED" : "all passed");
    return failures ? 1 : 0;
}
