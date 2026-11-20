// The commit gate's rules, every interleaving of a few threads - TODO 938.
//
// src/commit_gate_core.h decides who may come into a space's commit gate and whether
// one leaving wakes the waiters. This runs threads, each with a short script of takes
// and lets go, through every order the gate allows, the way commit_gate.h runs them:
// a thread that can't come in counts itself waiting and sleeps, and sleeps until a
// leaver wakes everyone; then it looks again. Nothing wakes by itself, so a wake that
// was needed and missed leaves threads asleep for good. It checks:
//   - shared and commit holders are never in together, and an alone holder is in alone;
//   - no order ends with a thread asleep that nobody will wake;
//   - a commit never comes in while a shared or alone holder waits;
//   - a thread holding it shared takes it shared again without waiting.
//
// It also runs rules that are known to be wrong and checks that it catches each:
//   - a reader that leaves last wakes nobody;
//   - a commit that leaves last wakes only commits;
//   - a reader let in beside a commit;
//   - an alone holder let in beside a commit;
//   - a commit let in past waiting readers;
//   - a reader kept out while an alone holder waits, which a reader taking it
//     again then never gets past.
#include "commit_gate_core.h"

#include <cstdio>
#include <functional>
#include <set>
#include <string>
#include <vector>

using namespace barch::commit_gate_core;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    struct rules {
        std::function<bool(const counts&, mode)> may_enter;
        std::function<bool(const counts&, mode)> wakes;
    };

    struct op {
        bool take;
        mode m;
    };
    using script = std::vector<op>;

    enum class phase { ready, asleep, awake };

    struct thread {
        size_t pc{0};
        phase at{phase::ready};
        size_t shared_held{0};
    };

    struct state {
        counts c;
        std::vector<thread> t;

        [[nodiscard]] std::vector<size_t> key() const {
            std::vector<size_t> k{c.readers, c.committers, (size_t) c.alone, c.waiting_readers,
                                  c.waiting_committers, c.waiting_alone};
            for (const auto& x : t) {
                k.push_back(x.pc);
                k.push_back((size_t) x.at);
                k.push_back(x.shared_held);
            }
            return k;
        }
    };

    size_t& waiting(counts& c, mode m) {
        return m == mode::shared ? c.waiting_readers : m == mode::commit ? c.waiting_committers : c.waiting_alone;
    }

    void enter(counts& c, mode m) {
        if (m == mode::shared) ++c.readers;
        else if (m == mode::commit) ++c.committers;
        else c.alone = true;
    }

    void leave(counts& c, mode m) {
        if (m == mode::shared) --c.readers;
        else if (m == mode::commit) --c.committers;
        else c.alone = false;
    }

    /** the first rule broken, over every order the scripts can run in, or empty */
    std::string explore(const rules& r, const std::vector<script>& scripts) {
        state start;
        start.t.resize(scripts.size());
        std::set<std::vector<size_t>> seen;
        std::vector<state> todo{start};
        while (!todo.empty()) {
            auto s = std::move(todo.back());
            todo.pop_back();
            if (!seen.insert(s.key()).second)
                continue;
            if (s.c.readers > 0 && s.c.committers > 0)
                return "a reader and a commit in together";
            if (s.c.alone && (s.c.readers > 0 || s.c.committers > 0))
                return "an alone holder not alone";
            bool done = true, moved = false;
            for (size_t i = 0; i < s.t.size(); ++i) {
                const auto& th = s.t[i];
                if (th.pc == scripts[i].size())
                    continue;
                done = false;
                if (th.at == phase::asleep)
                    continue;
                moved = true;
                state n = s;
                auto& me = n.t[i];
                const op o = scripts[i][th.pc];
                if (o.take) {
                    if (r.may_enter(n.c, o.m)) {
                        if (o.m == mode::commit && (n.c.waiting_readers > 0 || n.c.waiting_alone > 0))
                            return "a commit came in while a reader or an alone holder waits";
                        if (me.at == phase::awake)
                            --waiting(n.c, o.m);
                        enter(n.c, o.m);
                        me.at = phase::ready;
                        if (o.m == mode::shared)
                            ++me.shared_held;
                        ++me.pc;
                    } else {
                        if (o.m == mode::shared && me.shared_held > 0)
                            return "a thread holding it shared waited to take it again";
                        if (me.at == phase::ready)
                            ++waiting(n.c, o.m);
                        me.at = phase::asleep;
                    }
                } else {
                    leave(n.c, o.m);
                    if (o.m == mode::shared)
                        --me.shared_held;
                    ++me.pc;
                    if (r.wakes(n.c, o.m))
                        for (auto& x : n.t)
                            if (x.at == phase::asleep)
                                x.at = phase::awake;
                }
                todo.push_back(std::move(n));
            }
            if (!done && !moved)
                return "threads asleep that nobody will wake";
        }
        return {};
    }

    const op sh{true, mode::shared}, sh_out{false, mode::shared};
    const op cm{true, mode::commit}, cm_out{false, mode::commit};
    const op al{true, mode::alone}, al_out{false, mode::alone};

    const std::vector<script> menu{
            {sh, sh_out},
            {cm, cm_out},
            {al, al_out},
            {sh, sh, sh_out, sh_out},       // a read inside a read
            {cm, cm_out, cm, cm_out},
            {sh, sh_out, al, al_out},
    };

    /** the first fault over every choice of `threads` scripts from the menu */
    std::string every_choice(const rules& r, size_t threads, size_t* runs = nullptr) {
        std::vector<size_t> pick(threads, 0);
        while (true) {
            std::vector<script> scripts;
            for (auto p : pick)
                scripts.push_back(menu[p]);
            if (runs)
                ++*runs;
            auto got = explore(r, scripts);
            if (!got.empty())
                return got;
            size_t i = 0;
            while (i < threads && ++pick[i] == menu.size())
                pick[i++] = 0;
            if (i == threads)
                return {};
        }
    }

    std::string every_size(const rules& r, size_t* runs = nullptr) {
        for (size_t n = 1; n <= 4; ++n) {
            auto got = every_choice(r, n, runs);
            if (!got.empty())
                return got;
        }
        return {};
    }
}

int main() {
    const rules now{
            [](const counts& c, mode m) { return may_enter(c, m); },
            [](const counts& c, mode m) { return wakes(c, m); },
    };

    std::printf("the rules directly\n");
    {
        counts c;
        check(may_enter(c, mode::shared) && may_enter(c, mode::commit) && may_enter(c, mode::alone),
              "an empty gate lets anyone in");
        c.committers = 1;
        check(!may_enter(c, mode::shared), "a reader waits for a commit");
        check(may_enter(c, mode::commit), "commits go together");
        c.waiting_readers = 1;
        check(!may_enter(c, mode::commit), "a commit waits behind a waiting reader");
        c = {};
        c.readers = 1;
        c.waiting_alone = 1;
        check(may_enter(c, mode::shared), "a reader comes in past a waiting alone holder");
        c.readers = 0;
        check(wakes(c, mode::shared), "the last reader out wakes the alone holder");
    }

    std::printf("every interleaving\n");
    {
        size_t runs = 0;
        const auto got = every_size(now, &runs);
        if (!got.empty())
            std::printf("  %s\n", got.c_str());
        char line[128];
        std::snprintf(line, sizeof line, "as now (%zu choices of scripts, up to 4 threads)", runs);
        check(got.empty(), line);
    }

    std::printf("rules known to be wrong, each caught\n");
    {
        struct mutant {
            const char* what;
            rules r;
        };
        const std::vector<mutant> mutants{
                {"the last reader out wakes nobody", {now.may_enter, [](const counts& c, mode m) {
                    return m != mode::shared && wakes(c, m);
                }}},
                {"the last commit out wakes only commits", {now.may_enter, [](const counts& c, mode m) {
                    if (m == mode::commit)
                        return c.committers == 0 && c.waiting_committers > 0;
                    return wakes(c, m);
                }}},
                {"a reader let in beside a commit", {[](const counts& c, mode m) {
                    return m == mode::shared ? !c.alone : may_enter(c, m);
                }, now.wakes}},
                {"an alone holder let in beside a commit", {[](const counts& c, mode m) {
                    return m == mode::alone ? !c.alone && c.readers == 0 : may_enter(c, m);
                }, now.wakes}},
                {"a commit let in past waiting readers", {[](const counts& c, mode m) {
                    return m == mode::commit ? !c.alone && c.readers == 0 && c.waiting_alone == 0
                                             : may_enter(c, m);
                }, now.wakes}},
                {"a reader kept out while an alone holder waits", {[](const counts& c, mode m) {
                    return m == mode::shared ? may_enter(c, m) && c.waiting_alone == 0 : may_enter(c, m);
                }, now.wakes}},
        };
        for (const auto& m : mutants) {
            const auto got = every_size(m.r);
            char line[160];
            std::snprintf(line, sizeof line, "%s: %s", m.what, got.empty() ? "missed" : got.c_str());
            check(!got.empty(), line);
        }
    }

    std::printf(failures ? "FAILED\n" : "all passed\n");
    return failures ? 1 : 0;
}
