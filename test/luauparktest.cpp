// A Luau call's parks, raises and budget yields against the completions - TODO 942.
//
// src/luau_park_core.h decides what the pump does with a yield, where the next
// slice of a budget yield runs, and what a completion does. This runs every script
// of up to three steps, each step one of:
//   - a park: park_call, the work started, park_yield;
//   - a park_call that raises before the work starts, caught by the script;
//   - a park_call that raises after the work starts, caught by the script;
//   - a budget yield;
// on the pool, on the calling thread, and in overtime. Each started park gets two
// completions, as a fetch gets its answer and its timeout. Every order of the
// pump's steps, the completions' steps and the pool's runs is tried.
// It checks:
//   - every call finishes: no yield waits for a completion that never comes;
//   - a park's values go to that park's yield, and only after its work is done;
//   - a call is never resumed by two at once, or after it finished;
//   - a call on the calling thread never goes on the pool, and one in overtime
//     goes on the overtime pool.
//
// It also runs rules that are known to be wrong and checks that it catches each.
// One is the code as it was before TODO 942: a yield taken for a park's because the
// call is marked parked.
#include "luau_park_core.h"

#include <cstdio>
#include <functional>
#include <set>
#include <string>
#include <vector>

using namespace barch::luau_park_core;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    struct rules {
        std::function<on_yield(bool, bool, bool)> yield;
        std::function<slice_at(bool, bool, bool)> next_slice;
        std::function<on_complete(bool, bool)> complete;
        /** park_yield marks its yield */
        bool mark_yield{true};
        /** the pump clears the mark at each yield */
        bool clear_mark{true};
    };

    enum class step { park, raise_before, raise_after, budget };
    enum class where { idle, top, run, yield, finished };
    enum class setup { pool, here, overtime };

    constexpr int budget_yield = -1;
    constexpr int no_yield = -2;

    struct park {
        bool started{false};
        bool done{false};
        bool waiting{false};
        /** each completion: 0 to come, 1 to post the resume, 2 gone */
        int completion[2]{0, 0};
    };

    struct state {
        size_t pc{0};
        int phase{0};
        where at{where::top};
        bool running{true};
        int parked{-1};
        bool park_yielded{false};
        /** the yield the call sits at: a park's number, budget_yield or no_yield */
        int suspended{no_yield};
        std::vector<park> parks;
        int tasks{0};
        int overtime_tasks{0};

        std::string key() const {
            std::string k;
            k += char('0' + pc);
            k += char('0' + phase);
            k += char('0' + int(at));
            k += running ? 'r' : '-';
            k += char('a' + parked + 1);
            k += park_yielded ? 'y' : '-';
            k += char('a' + suspended + 2);
            for (const auto& p : parks) {
                k += '|';
                k += p.started ? 's' : '-';
                k += p.done ? 'd' : '-';
                k += p.waiting ? 'w' : '-';
                k += char('0' + p.completion[0]);
                k += char('0' + p.completion[1]);
            }
            k += '/';
            k += std::to_string(tasks) + ',' + std::to_string(overtime_tasks);
            return k;
        }
    };

    struct world {
        const rules& r;
        const std::vector<step>& script;
        setup where_runs;
        std::set<std::string> seen;
        std::string bad;

        void fail(const std::string& what) {
            if (bad.empty()) bad = what;
        }

        // one step of the pump
        void pump(state& s) {
            switch (s.at) {
            case where::top:
                // a call coming back from a park takes what it waited for
                if (s.parked >= 0) {
                    if (!s.parks[s.parked].done) {
                        s.running = false;
                        s.at = where::idle;
                        return;
                    }
                    if (s.suspended != s.parked)
                        fail("a park's values went to another yield");
                    s.parked = -1;
                } else if (s.suspended >= 0) {
                    fail("a park's yield resumed without its values");
                }
                s.suspended = no_yield;
                s.at = where::run;
                return;
            case where::run: {
                if (s.pc == script.size()) {
                    // finish_job drops the park
                    s.parked = -1;
                    s.running = false;
                    s.at = where::finished;
                    return;
                }
                switch (script[s.pc]) {
                case step::park:
                    if (s.phase == 0) {
                        s.parked = (int) s.parks.size();
                        s.parks.emplace_back();
                        s.phase = 1;
                    } else if (s.phase == 1) {
                        s.parks[s.parked].started = true;
                        s.phase = 2;
                    } else {
                        if (r.mark_yield) s.park_yielded = true;
                        s.suspended = s.parked;
                        ++s.pc;
                        s.phase = 0;
                        s.at = where::yield;
                    }
                    return;
                case step::raise_before:
                    s.parked = (int) s.parks.size();
                    s.parks.emplace_back();
                    ++s.pc;
                    return;
                case step::raise_after:
                    if (s.phase == 0) {
                        s.parked = (int) s.parks.size();
                        s.parks.emplace_back();
                        s.phase = 1;
                    } else {
                        s.parks[s.parked].started = true;
                        ++s.pc;
                        s.phase = 0;
                    }
                    return;
                case step::budget:
                    s.suspended = budget_yield;
                    ++s.pc;
                    s.at = where::yield;
                    return;
                }
                return;
            }
            case where::yield: {
                // under the park's lock
                const bool parked = s.parked >= 0;
                const bool done = parked && s.parks[s.parked].done;
                auto act = r.yield(parked, s.park_yielded, done);
                if (r.clear_mark) s.park_yielded = false;
                if ((act == on_yield::wait || act == on_yield::resume_now) && !parked) {
                    fail("a budget yield taken for a park's");
                    return;
                }
                if (act == on_yield::wait) {
                    s.parks[s.parked].waiting = true;
                    s.running = false;
                    s.at = where::idle;
                    return;
                }
                if (act == on_yield::resume_now) {
                    s.at = where::top;
                    return;
                }
                if (act == on_yield::stale_budget)
                    s.parked = -1;
                switch (r.next_slice(where_runs == setup::here, false, where_runs == setup::overtime)) {
                case slice_at::here:
                    if (where_runs != setup::here)
                        fail("a pool call's next slice held its thread");
                    s.at = where::top;
                    return;
                case slice_at::pool:
                    if (where_runs == setup::here)
                        fail("a call on the calling thread went on the pool");
                    if (where_runs == setup::overtime)
                        fail("an overtime call went on the normal pool");
                    ++s.tasks;
                    break;
                case slice_at::overtime_pool:
                    if (where_runs == setup::here)
                        fail("a call on the calling thread went on the pool");
                    if (where_runs == setup::pool)
                        fail("a call in time went on the overtime pool");
                    ++s.overtime_tasks;
                    break;
                }
                s.running = false;
                s.at = where::idle;
                return;
            }
            case where::idle:
            case where::finished:
                return;
            }
        }

        void take_task(state& s, int& from) {
            --from;
            if (s.at == where::finished) {
                fail("a call resumed after it finished");
                return;
            }
            if (s.running) {
                fail("a call resumed by two at once");
                return;
            }
            s.running = true;
            s.at = where::top;
        }

        void complete(state& s, int k, int j) {
            auto& p = s.parks[k];
            if (p.completion[j] == 0) {
                // under the park's lock
                switch (r.complete(p.done, p.waiting)) {
                case on_complete::ignore:
                    p.completion[j] = 2;
                    return;
                case on_complete::record:
                    p.done = true;
                    p.completion[j] = 2;
                    return;
                case on_complete::record_and_resume:
                    p.done = true;
                    p.waiting = false;
                    p.completion[j] = 1;
                    return;
                }
            }
            // the resume, posted after the lock
            ++s.tasks;
            p.completion[j] = 2;
        }

        void explore(const state& s) {
            if (!bad.empty()) return;
            if (!seen.insert(s.key()).second) return;
            bool moved = false;
            if (s.running) {
                state n = s;
                pump(n);
                moved = true;
                explore(n);
            }
            if (s.tasks > 0) {
                state n = s;
                take_task(n, n.tasks);
                moved = true;
                explore(n);
            }
            if (s.overtime_tasks > 0) {
                state n = s;
                take_task(n, n.overtime_tasks);
                moved = true;
                explore(n);
            }
            for (int k = 0; k < (int) s.parks.size(); ++k) {
                if (!s.parks[k].started) continue;
                for (int j = 0; j < 2; ++j) {
                    if (s.parks[k].completion[j] == 2) continue;
                    state n = s;
                    complete(n, k, j);
                    moved = true;
                    explore(n);
                }
            }
            if (!moved && s.at != where::finished)
                fail("a call never finished");
        }
    };

    const char* name(step s) {
        switch (s) {
        case step::park: return "park";
        case step::raise_before: return "raise before start";
        case step::raise_after: return "raise after start";
        case step::budget: return "budget";
        }
        return "?";
    }

    const char* name(setup s) {
        switch (s) {
        case setup::pool: return "pool";
        case setup::here: return "calling thread";
        case setup::overtime: return "overtime";
        }
        return "?";
    }

    std::vector<std::vector<step>> scripts() {
        const step all[] = {step::park, step::raise_before, step::raise_after, step::budget};
        std::vector<std::vector<step>> out{{}};
        for (size_t from = 0, len = 0; len < 3; ++len) {
            const size_t to = out.size();
            for (size_t i = from; i < to; ++i)
                for (step s : all) {
                    auto n = out[i];
                    n.push_back(s);
                    out.push_back(n);
                }
            from = to;
        }
        return out;
    }

    // the first failure over every script and setup, or empty
    std::string all(const rules& r) {
        for (const auto& script : scripts())
            for (setup at : {setup::pool, setup::here, setup::overtime}) {
                world w{r, script, at, {}, {}};
                w.explore(state{});
                if (!w.bad.empty()) {
                    std::string got = w.bad + " (" + name(at) + ":";
                    for (step s : script) got += std::string(" ") + name(s);
                    return got + ")";
                }
            }
        return {};
    }
}

int main() {
    const rules now{yield, next_slice, complete, true, true};

    std::printf("every script, setup and order\n");
    {
        size_t states = 0;
        for (const auto& script : scripts())
            for (setup at : {setup::pool, setup::here, setup::overtime}) {
                world w{now, script, at, {}, {}};
                w.explore(state{});
                states += w.seen.size();
            }
        std::printf("  %zu scripts, %zu states\n", scripts().size(), states);
        const auto got = all(now);
        if (!got.empty())
            std::printf("  %s\n", got.c_str());
        check(got.empty(), "as now");
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
                {"a marked park makes a yield a park's, as before TODO 942", with([](rules& r) {
                    r.yield = [](bool parked, bool, bool done) { return yield(parked, true, done); };
                })},
                {"park_yield leaves its yield unmarked", with([](rules& r) { r.mark_yield = false; })},
                {"the mark kept past its yield", with([](rules& r) { r.clear_mark = false; })},
                {"a stale park taken as a finished one", with([](rules& r) {
                    r.yield = [](bool parked, bool yielded, bool done) {
                        auto a = yield(parked, yielded, done);
                        return a == on_yield::stale_budget ? on_yield::resume_now : a;
                    };
                })},
                {"a stale park kept", with([](rules& r) {
                    r.yield = [](bool parked, bool yielded, bool done) {
                        auto a = yield(parked, yielded, done);
                        return a == on_yield::stale_budget ? on_yield::wait : a;
                    };
                })},
                {"a finished park waited for", with([](rules& r) {
                    r.yield = [](bool parked, bool yielded, bool done) {
                        auto a = yield(parked, yielded, done);
                        return a == on_yield::resume_now ? on_yield::wait : a;
                    };
                })},
                {"an unfinished park resumed", with([](rules& r) {
                    r.yield = [](bool parked, bool yielded, bool done) {
                        auto a = yield(parked, yielded, done);
                        return a == on_yield::wait ? on_yield::resume_now : a;
                    };
                })},
                {"a completion resumes a call that has not yielded", with([](rules& r) {
                    r.complete = [](bool done, bool) { return complete(done, true); };
                })},
                {"a completion never resumes", with([](rules& r) {
                    r.complete = [](bool done, bool) { return complete(done, false); };
                })},
                {"a second completion resumes again", with([](rules& r) {
                    r.complete = [](bool done, bool waiting) {
                        return done ? on_complete::record_and_resume : complete(done, waiting);
                    };
                })},
                {"the calling thread's slice goes on the pool", with([](rules& r) {
                    r.next_slice = [](bool, bool http, bool ot) { return next_slice(false, http, ot); };
                })},
                {"overtime ignored", with([](rules& r) {
                    r.next_slice = [](bool in, bool http, bool) { return next_slice(in, http, false); };
                })},
                {"every slice held on its thread", with([](rules& r) {
                    r.next_slice = [](bool, bool, bool) { return slice_at::here; };
                })},
        };
        for (const auto& m : mutants) {
            const auto got = all(m.r);
            char line[240];
            std::snprintf(line, sizeof line, "%s: %s", m.what, got.empty() ? "missed" : got.c_str());
            check(!got.empty(), line);
        }
    }

    std::printf(failures ? "FAILED\n" : "all passed\n");
    return failures ? 1 : 0;
}
