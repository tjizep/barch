// A blocked client's lists, wakes and the space it parked in - TODO 944.
//
// src/block_core.h decides whether a session goes on a key's list, whether an empty
// list goes, whether a write wakes a waiter, which keys a write finds by name, what a
// woken session does once its handler ran, and whether the caller keeps the park's
// space. This runs two clients, each a BLPOP that names its key twice or a CALLF that
// waits on its own wake key, against every order of:
//   - each client parking, in the space open at the time. A BLPOP with something to
//     pop takes it and does not park;
//   - a push to the key, in the space open now, which wakes the key's first waiter;
//   - an UNLOAD, which wakes every waiter in the space and opens it again from its
//     files;
//   - a CALLF's job ending, which wakes it through the space it kept;
//   - a woken session running its handler in the space it parked in.
// With up to two pushes and two unloads. It checks:
//   - a session is on a list at most once, and only while it is parked;
//   - a list is kept only while someone waits on it;
//   - a BLPOP never parks in a space that went;
//   - nothing is refused in a space that is still open;
//   - a CALLF is never answered while its job runs;
//   - a client that was answered holds no space;
//   - at the end nothing is left parked that something could still have woken: no
//     BLPOP with something to pop, and no CALLF whose job ended.
//
// It also runs rules that are known to be wrong and checks that it catches each. Two
// are the code as it was before TODO 944: a retry that always parks again, and a caller
// that keeps the park's space.
#include "block_core.h"

#include <cstdint>
#include <cstdio>
#include <functional>
#include <set>
#include <string>
#include <vector>

using namespace barch::block_core;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    struct rules {
        std::function<bool(bool)> adds = barch::block_core::adds;
        std::function<bool(size_t)> drops_list = barch::block_core::drops_list;
        std::function<bool(size_t)> wakes = barch::block_core::wakes;
        std::function<bool(std::string_view)> by_name = barch::block_core::by_name;
        std::function<after_wake(bool, bool, bool)> after = barch::block_core::after;
        std::function<bool(after_wake, bool)> keeps_space = barch::block_core::keeps_space;
    };

    enum class kind : uint8_t { blpop, callf };
    enum class phase : uint8_t { idle, parked, woken, answered, refused };

    constexpr int clients = 2;
    constexpr int spaces = 3;
    const std::string list_key = "k";
    const std::string wake_key = std::string("\x01") + "function.wake.1";

    struct list {
        bool present{false};
        std::vector<uint8_t> waiting;
    };

    struct client {
        phase ph{phase::idle};
        uint8_t space{0};
        bool holds{false};
        bool job_running{false};
    };

    struct state {
        uint8_t now{0};   // the space open under the name
        uint8_t pushes{0};
        uint8_t unloads{0};
        bool gone[spaces]{};
        uint8_t items[spaces]{};
        // the key's list and the CALLF wake key's list, in each space
        list keyed[spaces];
        list wake[spaces];
        client c[clients];

        [[nodiscard]] std::string key() const {
            std::string k;
            k += char(now);
            k += char(pushes);
            k += char(unloads);
            for (int g = 0; g < spaces; ++g) {
                k += char(gone[g]);
                k += char(items[g]);
                for (const auto* l : {&keyed[g], &wake[g]}) {
                    k += char(l->present);
                    k += char(l->waiting.size());
                    for (auto w : l->waiting) k += char(w);
                }
            }
            for (const auto& x : c) {
                k += char(x.ph);
                k += char(x.space);
                k += char(x.holds);
                k += char(x.job_running);
            }
            return k;
        }
    };

    struct explorer {
        const rules& r;
        kind kinds[clients];
        std::set<std::string> seen;
        std::string fault;
        size_t states{0};

        list& list_of(state& s, int i) {
            return kinds[i] == kind::blpop ? s.keyed[s.c[i].space] : s.wake[s.c[i].space];
        }
        const std::string& key_of(int i) const {
            return kinds[i] == kind::blpop ? list_key : wake_key;
        }

        // add_rpc_block
        void add(list& l, uint8_t who) {
            bool there = false;
            for (auto w : l.waiting) there = there || w == who;
            if (r.adds(there)) {
                l.waiting.push_back(who);
                l.present = true;
            }
        }
        // unblock_key_: every copy goes
        void remove_all(list& l, uint8_t who) {
            std::vector<uint8_t> left;
            for (auto w : l.waiting)
                if (w != who) left.push_back(w);
            l.waiting = left;
            if (r.drops_list(l.waiting.size())) l.present = false;
        }
        // call_unblock: the first only
        void wake_first(state& s, list& l) {
            if (!l.present || !r.wakes(l.waiting.size())) return;
            if (l.waiting.empty()) return;
            const uint8_t first = l.waiting.front();
            l.waiting.erase(l.waiting.begin());
            if (r.drops_list(l.waiting.size())) l.present = false;
            if (s.c[first].ph == phase::parked) s.c[first].ph = phase::woken;
        }

        void park(state& s, int i) {
            auto& x = s.c[i];
            x.ph = phase::parked;
            x.holds = true;
            add(list_of(s, i), (uint8_t) i);
            // a BLPOP names its key twice
            if (kinds[i] == kind::blpop) add(list_of(s, i), (uint8_t) i);
        }

        // the woken session runs its handler in the space it parked in
        void run(state& s, int i) {
            auto& x = s.c[i];
            bool retry;
            if (kinds[i] == kind::blpop) {
                retry = s.items[x.space] == 0;
                if (!retry) --s.items[x.space];
            } else {
                retry = x.job_running;
            }
            const auto what = r.after(retry, s.gone[x.space], r.by_name(key_of(i)));
            if (what == after_wake::park_again) {
                park(s, i);
                // the caller keeps block_space, or not
                x.holds = r.keeps_space(what, true);
                return;
            }
            // write_result, erase_blocks
            remove_all(list_of(s, i), (uint8_t) i);
            x.ph = what == after_wake::answer ? phase::answered : phase::refused;
            x.holds = r.keeps_space(what, true);
            if (what == after_wake::refuse && !s.gone[x.space])
                fault = "refused in a space that is still open";
            if (kinds[i] == kind::callf && x.job_running)
                fault = "a CALLF answered while its job runs";
        }

        void invariants(const state& s) {
            for (int g = 0; g < spaces && fault.empty(); ++g)
                for (const auto* l : {&s.keyed[g], &s.wake[g]}) {
                    if (l->present && l->waiting.empty())
                        fault = "a list kept with nobody on it";
                    if (!l->present && !l->waiting.empty())
                        fault = "a list dropped with waiters on it";
                    for (size_t a = 0; a < l->waiting.size(); ++a) {
                        for (size_t b = a + 1; b < l->waiting.size(); ++b)
                            if (l->waiting[a] == l->waiting[b])
                                fault = "a session on a list twice";
                        const auto& w = s.c[l->waiting[a]];
                        if (w.ph != phase::parked || w.space != g)
                            fault = "a session on a list it is not parked on";
                    }
                }
            for (int i = 0; i < clients && fault.empty(); ++i) {
                const auto& x = s.c[i];
                if (kinds[i] == kind::blpop && x.ph == phase::parked && s.gone[x.space])
                    fault = "a BLPOP parked in a space that went";
                if ((x.ph == phase::answered || x.ph == phase::refused) && x.holds)
                    fault = "an answered client holds the space it parked in";
                if ((x.ph == phase::parked || x.ph == phase::woken) && !x.holds)
                    fault = "a parked client lost the space it parked in";
            }
        }

        void terminal(const state& s) {
            for (int i = 0; i < clients && fault.empty(); ++i) {
                const auto& x = s.c[i];
                if (x.ph != phase::parked) continue;
                if (kinds[i] == kind::blpop && s.items[x.space] > 0)
                    fault = "a BLPOP left parked with something to pop";
                if (kinds[i] == kind::callf && !x.job_running)
                    fault = "a CALLF left parked after its job ended";
            }
        }

        void visit(const state& s) {
            if (!fault.empty()) return;
            if (!seen.insert(s.key()).second) return;
            ++states;
            invariants(s);
            if (!fault.empty()) return;
            bool any = false;
            auto go = [&](const state& t) {
                any = true;
                visit(t);
            };
            for (int i = 0; i < clients; ++i) {
                if (s.c[i].ph == phase::idle) {
                    state t = s;
                    t.c[i].space = t.now;
                    if (kinds[i] == kind::blpop && t.items[t.now] > 0) {
                        --t.items[t.now];
                        t.c[i].ph = phase::answered;
                    } else {
                        if (kinds[i] == kind::callf) t.c[i].job_running = true;
                        park(t, i);
                    }
                    go(t);
                }
                if (s.c[i].ph == phase::woken) {
                    state t = s;
                    run(t, i);
                    go(t);
                }
                if (kinds[i] == kind::callf && s.c[i].job_running) {
                    // wake(): through the space the job kept
                    state t = s;
                    t.c[i].job_running = false;
                    auto& l = t.wake[t.c[i].space];
                    for (size_t at = 0; at < l.waiting.size(); ++at)
                        if (l.waiting[at] == i) {
                            l.waiting.erase(l.waiting.begin() + (long) at);
                            if (r.drops_list(l.waiting.size())) l.present = false;
                            t.c[i].ph = phase::woken;
                            break;
                        }
                    go(t);
                }
            }
            if (s.pushes < 2) {
                state t = s;
                ++t.pushes;
                ++t.items[t.now];
                wake_first(t, t.keyed[t.now]);
                go(t);
            }
            if (s.unloads < 2) {
                // fail_foreign_flights: every waiter woken, the lists cleared. The
                // space opens again from its files
                state t = s;
                ++t.unloads;
                const uint8_t g = t.now;
                t.gone[g] = true;
                for (auto* l : {&t.keyed[g], &t.wake[g]}) {
                    for (auto w : l->waiting)
                        if (t.c[w].ph == phase::parked) t.c[w].ph = phase::woken;
                    *l = list{};
                }
                t.now = g + 1;
                t.items[t.now] = t.items[g];
                go(t);
            }
            if (!any) terminal(s);
        }
    };

    // every pair of clients; empty if it all holds
    std::string all(const rules& r, size_t* states = nullptr) {
        size_t total = 0;
        const kind both[] = {kind::blpop, kind::callf};
        for (auto a : both)
            for (auto b : both) {
                explorer e{r, {a, b}, {}, {}, 0};
                e.visit(state{});
                total += e.states;
                if (!e.fault.empty()) {
                    auto name = [](kind k) { return k == kind::blpop ? "BLPOP" : "CALLF"; };
                    return std::string(name(a)) + " and " + name(b) + ": " + e.fault;
                }
            }
        if (states) *states = total;
        return {};
    }
}

int main() {
    const rules now;

    std::printf("two clients against every order of parks, pushes, unloads and wakes\n");
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
                {"a retry always parks again, as before TODO 944", with([](rules& r) {
                    r.after = [](bool retry, bool, bool) {
                        return retry ? after_wake::park_again : after_wake::answer;
                    };
                })},
                {"the caller keeps the park's space, as before TODO 944", with([](rules& r) {
                    r.keeps_space = [](after_wake, bool) { return true; };
                })},
                {"the caller drops the space when it parks again", with([](rules& r) {
                    r.keeps_space = [](after_wake, bool) { return false; };
                })},
                {"CALLF's wake key taken as found by name", with([](rules& r) {
                    r.by_name = [](std::string_view) { return true; };
                })},
                {"no key taken as found by name", with([](rules& r) {
                    r.by_name = [](std::string_view) { return false; };
                })},
                {"refused whether or not the space went", with([](rules& r) {
                    r.after = [](bool retry, bool, bool by) { return after(retry, true, by); };
                })},
                {"a session added however often it named the key", with([](rules& r) {
                    r.adds = [](bool) { return true; };
                })},
                {"a list kept when empty", with([](rules& r) {
                    r.drops_list = [](size_t) { return false; };
                })},
                {"a list dropped with waiters on it", with([](rules& r) {
                    r.drops_list = [](size_t) { return true; };
                })},
                {"a write wakes nobody", with([](rules& r) {
                    r.wakes = [](size_t) { return false; };
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
