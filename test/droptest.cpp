// A dropped space's files, and a connection that chose it - TODO 945.
//
// src/drop_core.h decides whether a drop deletes the files at once, whether the last
// holder of a dropped space deletes the files under its name, and whether a connection
// whose space went moves to the space under the name. This runs one name, a connection
// that chose it with USE, and another that names it with space:CMD, against every
// order of:
//   - the connection writing, or saving, after it moves if its space went;
//   - the connection beginning and ending a transaction;
//   - the connection closing, which lets go of its space;
//   - the other writing and saving under the name, which opens the name if nothing
//     holds it;
//   - KSPACE DROP and UNLOAD of the space under the name.
// A space opened from the files holds what they hold. It checks:
//   - a space open under the name never holds a key written in a dropped space;
//   - no file of a dropped space is left under the name;
//   - a release never deletes the files a space that was not dropped saved;
//   - the connection's writes outside a transaction land in an open space;
//   - its space doesn't change inside a transaction.
//
// It also runs rules that are known to be wrong and checks that it catches each. One is
// the code as it was before TODO 945: no delete at the drop, a release that always
// deletes, and a connection that never moves.
#include "drop_core.h"

#include <cstdint>
#include <cstdio>
#include <functional>
#include <set>
#include <string>
#include <vector>

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    struct rules {
        std::function<bool()> deletes_at_drop = barch::drop_core::deletes_at_drop;
        std::function<bool(uint64_t, uint64_t)> deletes_at_release = barch::drop_core::deletes_at_release;
        std::function<bool(bool, bool)> follows_name = barch::drop_core::follows_name;
    };

    constexpr int spaces = 4;           // a space built under the name, 0 the first
    constexpr uint8_t none = 0xff;

    struct state {
        uint8_t now{0};                 // the space under the name, or none
        uint8_t built{1};               // spaces built so far
        uint8_t data[spaces]{};         // which spaces' writes each holds, a bit each
        bool dropped[spaces]{};
        bool released[spaces]{};
        uint8_t opens_at_drop[spaces]{};
        uint8_t opens{0};               // opens of the name since its first drop
        bool ever_dropped{false};
        uint8_t dropped_bits{0};        // what the dropped spaces held when dropped
        uint8_t file{0};                // what the files under the name hold
        uint8_t file_by{none};          // the space that saved them, or none
        // the connection that chose the name
        bool open{true};
        uint8_t space{0};
        bool in_tx{false};
        uint8_t tx_space{0};
        // the bounds
        uint8_t writes{0}, saves{0}, other{0}, drops{0}, unloads{0}, txs{0};

        [[nodiscard]] std::string key() const {
            std::string k;
            for (auto v : {now, built, opens, (uint8_t) ever_dropped, dropped_bits, file, file_by,
                           (uint8_t) open, space, (uint8_t) in_tx, tx_space,
                           writes, saves, other, drops, unloads, txs})
                k += char(v);
            for (int g = 0; g < spaces; ++g) {
                k += char(data[g]);
                k += char(dropped[g]);
                k += char(released[g]);
                k += char(opens_at_drop[g]);
            }
            return k;
        }
    };

    struct explorer {
        const rules& r;
        std::set<std::string> seen;
        std::string fault;
        size_t states{0};

        [[nodiscard]] static bool gone(const state& s, uint8_t g) {
            return s.now != g;
        }
        [[nodiscard]] static bool held(const state& s, uint8_t g) {
            return s.now == g || (s.open && s.space == g);
        }

        // get_keyspace: what is under the name, or a space built from the files
        bool open_name(state& s) {
            if (s.now != none)
                return true;
            if (s.built >= spaces)
                return false;
            const uint8_t g = s.built++;
            s.data[g] = s.file;
            if (s.ever_dropped)
                ++s.opens;
            s.now = g;
            return true;
        }

        // the last holder let go: ~shard
        void release(state& s, uint8_t g) {
            if (held(s, g) || s.released[g])
                return;
            s.released[g] = true;
            if (s.dropped[g] && r.deletes_at_release(s.opens_at_drop[g], s.opens) && s.file_by != none) {
                if (!s.dropped[s.file_by] && fault.empty())
                    fault = "a release deleted the files of a space that was not dropped";
                s.file = 0;
                s.file_by = none;
            }
        }

        // the start of each command: run_params
        bool follow(state& s) {
            if (r.follows_name(gone(s, s.space), s.in_tx)) {
                const uint8_t was = s.space;
                if (!open_name(s))
                    return false;
                s.space = s.now;
                release(s, was);
            }
            return true;
        }

        void invariants(const state& s) {
            if (!fault.empty())
                return;
            if (s.now != none && (s.data[s.now] & s.dropped_bits))
                fault = "a space under the name holds a key written in a dropped space";
            else if (s.file_by != none && s.dropped[s.file_by])
                fault = "a file of a dropped space is left under the name";
            else if (s.in_tx && s.space != s.tx_space)
                fault = "the connection's space changed inside a transaction";
        }

        void go(const state& t) {
            invariants(t);
            visit(t);
        }

        void visit(const state& s) {
            if (!fault.empty() || !seen.insert(s.key()).second)
                return;
            ++states;
            if (s.open && s.writes < 2) {
                state t = s;
                ++t.writes;
                if (follow(t)) {
                    if (!t.in_tx) {
                        if (gone(t, t.space) && fault.empty())
                            fault = "a write outside a transaction landed in a space that went";
                        t.data[t.space] |= uint8_t(1u << t.space);
                    }
                    go(t);
                }
            }
            if (s.open && !s.in_tx && s.saves < 1) {
                state t = s;
                ++t.saves;
                if (follow(t)) {
                    t.file = t.data[t.space];
                    t.file_by = t.space;
                    go(t);
                }
            }
            if (s.open && s.txs < 1) {
                state t = s;
                if (t.in_tx) {
                    t.in_tx = false;
                    ++t.txs;
                } else if (follow(t)) {
                    t.in_tx = true;
                    t.tx_space = t.space;
                }
                go(t);
            }
            if (s.open && !s.in_tx) {
                state t = s;
                t.open = false;
                release(t, t.space);
                go(t);
            }
            if (s.other < 2) {
                // space:SET and space:SAVE, from a connection that holds nothing
                state t = s;
                ++t.other;
                if (open_name(t)) {
                    t.data[t.now] |= uint8_t(1u << t.now);
                    t.file = t.data[t.now];
                    t.file_by = t.now;
                    go(t);
                }
            }
            if (s.now != none && s.drops < 2) {
                state t = s;
                ++t.drops;
                const uint8_t g = t.now;
                t.dropped[g] = true;
                t.ever_dropped = true;
                t.opens_at_drop[g] = t.opens;
                t.dropped_bits |= t.data[g];
                if (r.deletes_at_drop()) {
                    t.file = 0;
                    t.file_by = none;
                }
                t.now = none;
                release(t, g);
                go(t);
            }
            if (s.now != none && s.unloads < 1) {
                state t = s;
                ++t.unloads;
                const uint8_t g = t.now;
                t.now = none;
                release(t, g);
                go(t);
            }
        }
    };

    // empty if it all holds
    std::string all(const rules& r, size_t* states = nullptr) {
        explorer e{r, {}, {}, 0};
        e.visit(state{});
        if (states) *states = e.states;
        return e.fault;
    }
}

int main() {
    const rules now;

    std::printf("a name and a connection that chose it, against every order of events\n");
    {
        size_t states = 0;
        const auto got = all(now, &states);
        if (!got.empty())
            std::printf("  %s\n", got.c_str());
        char line[120];
        std::snprintf(line, sizeof line, "as now (%zu states)", states);
        check(got.empty(), line);
    }

    std::printf("rules known to be wrong, each caught\n");
    {
        struct mutant {
            const char* what;
            rules r;
        };
        auto with = [&](auto change) {
            rules r = now;
            change(r);
            return r;
        };
        const std::vector<mutant> mutants{
                {"as before TODO 945", with([](rules& r) {
                    r.deletes_at_drop = [] { return false; };
                    r.deletes_at_release = [](uint64_t, uint64_t) { return true; };
                    r.follows_name = [](bool, bool) { return false; };
                })},
                {"no delete at the drop", with([](rules& r) {
                    r.deletes_at_drop = [] { return false; };
                })},
                {"a release that always deletes", with([](rules& r) {
                    r.deletes_at_release = [](uint64_t, uint64_t) { return true; };
                })},
                {"a connection that never moves", with([](rules& r) {
                    r.follows_name = [](bool, bool) { return false; };
                })},
                {"a connection that moves inside a transaction", with([](rules& r) {
                    r.follows_name = [](bool gone, bool) { return gone; };
                })},
        };
        for (const auto& m : mutants) {
            const auto got = all(m.r);
            char line[400];
            std::snprintf(line, sizeof line, "%s: %s", m.what, got.empty() ? "missed" : got.c_str());
            check(!got.empty(), line);
        }
    }

    std::printf(failures ? "FAILED\n" : "all passed\n");
    return failures ? 1 : 0;
}
