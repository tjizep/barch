// A streamed reply's queues, waits and writes - TODO 943.
//
// src/stream_core.h decides when a worker waits, when it posts stream_next, what
// stream_next does, and what a write's completion does. This runs a worker that queues
// one to three pieces of one to three bytes each and then flushes, with the high water
// at 2, against every order of:
//   - stream_next runs on the socket's thread, posted or after a completion;
//   - each write completing with one byte taken, all of it taken, or an error;
//   - the client resetting, so every write from then on fails;
//   - the worker's wait timing out, and the shutdown it posts;
//   - the server stopping, which fails the stream and stops the socket's thread.
// Each with and without a timeout, a reset and a stop to use. It checks:
//   - the worker never hangs: it finishes, or gives up once the stream failed;
//   - a worker that finishes had every byte taken, and nothing failed;
//   - the backlog is what is queued plus what is not yet written;
//   - a worker queues only under the high water;
//   - one write or posted stream_next at a time;
//   - no write starts once the stream failed;
//   - a failed write fails the stream;
//   - a wait times out only when the client took nothing;
//   - one net error for a stream;
//   - a stream that failed, with the server not stopping, ends with the session let
//     go, and nothing left busy.
//
// It also runs rules that are known to be wrong and checks that it catches each. One
// is the code as it was before TODO 943: a failed write that does not let go.
#include "stream_core.h"

#include <cstdio>
#include <functional>
#include <set>
#include <string>
#include <vector>

using namespace barch::stream_core;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    struct rules {
        std::function<on_wait(bool, uint64_t, uint64_t)> wait_step = barch::stream_core::wait_step;
        std::function<bool(bool)> posts_next = barch::stream_core::posts_next;
        std::function<on_next(bool, bool, bool)> next = barch::stream_core::next;
        std::function<after_write(bool, bool, uint64_t, uint64_t)> written = barch::stream_core::written;
    };

    constexpr uint64_t high_water = 2;

    enum class phase : uint8_t { running, waiting, done, gave_up };

    struct state {
        // the worker
        uint8_t at{0};
        phase ph{phase::running};
        uint64_t seen{0};        // stream_progress when it began to wait
        uint64_t took_seen{0};   // bytes taken when it began to wait
        // under the stream's lock
        uint64_t backlog{0};
        uint64_t pending{0};
        bool failed{false};
        bool busy{false};
        uint64_t progress{0};
        // the socket's thread
        uint64_t writing{0};
        uint64_t written{0};
        uint8_t posted{0};
        bool write_out{false};
        uint64_t piece{0};
        uint8_t shutdowns_posted{0};
        bool shut{false};
        bool let_go{false};
        // the rest of the world
        bool gone{false};        // the client reset: every write fails
        bool stopped{false};     // the server stopped, and the socket's thread with it
        uint8_t net_errors{0};
        uint64_t took{0};        // bytes the client took
        uint64_t queued{0};      // bytes the worker queued
        uint8_t timeouts{0}, resets{0}, stops{0};

        std::string key() const {
            std::string k;
            auto put = [&](uint64_t v) { k += std::to_string(v); k += ','; };
            put(at); put((uint64_t) ph); put(seen); put(took_seen); put(backlog); put(pending);
            put(failed); put(busy); put(progress); put(writing); put(written); put(posted);
            put(write_out); put(piece); put(shutdowns_posted); put(shut); put(let_go); put(gone);
            put(stopped); put(net_errors); put(took); put(queued); put(timeouts); put(resets);
            put(stops);
            return k;
        }
    };

    struct limits {
        uint8_t timeouts, resets, stops;
    };

    struct explorer {
        const rules& r;
        const std::vector<uint64_t>& pieces;
        limits lim;
        std::set<std::string> seen;
        std::string fault;
        size_t states{0};

        void fail(const state& s, const char* what) {
            if (fault.empty())
                fault = std::string(what) + " at " + s.key();
        }

        uint64_t total() const {
            uint64_t t = 0;
            for (auto p : pieces) t += p;
            return t;
        }

        bool moved(const state& s, uint64_t below) const {
            return s.failed || s.backlog < below || s.progress != s.seen;
        }

        uint64_t below(const state& s) const {
            return s.at < pieces.size() ? high_water : 1;
        }

        // stream_next, on the socket's thread
        void next(state& s) {
            switch (r.next(s.failed, s.written >= s.writing, s.pending == 0)) {
                case on_next::stop:
                    s.pending = 0;
                    s.writing = 0;
                    s.written = 0;
                    s.backlog = 0;
                    s.busy = false;
                    return;
                case on_next::swap:
                    s.writing = s.pending;
                    s.pending = 0;
                    s.written = 0;
                    break;
                case on_next::write:
                    break;
            }
            if (s.failed) fail(s, "a write starts after the stream failed");
            if (s.write_out) fail(s, "two writes out at once");
            s.piece = s.writing - s.written;
            if (s.piece == 0) fail(s, "a write of nothing");
            s.write_out = true;
        }

        void complete(state& s, bool error, uint64_t length) {
            s.write_out = false;
            s.written += length;
            s.took += length;
            if (error) {
                s.writing = 0;
                s.written = 0;
            }
            auto w = r.written(error, s.failed, s.backlog, length);
            if (w.count_error) ++s.net_errors;
            s.failed = w.failed;
            s.backlog = w.backlog;
            if (w.progress) ++s.progress;
            if (error && !s.failed) fail(s, "a failed write leaves the stream open");
            if (w.let_go) {
                s.shut = true;
                s.let_go = true;
            }
            next(s);
        }

        void invariants(const state& s) {
            if (s.posted + (s.write_out ? 1 : 0) > 1)
                fail(s, "two stream_next chains at once");
            if (!s.failed && s.backlog != s.pending + (s.writing - s.written))
                fail(s, "the backlog is not what is queued and unwritten");
            if (s.backlog > s.queued)
                fail(s, "the backlog is more than was queued");
            if (s.net_errors > 1)
                fail(s, "more than one net error for a stream");
            if (s.took > s.queued)
                fail(s, "the client took more than was queued");
        }

        void terminal(const state& s) {
            if (s.ph == phase::running || s.ph == phase::waiting) {
                fail(s, "the worker hangs");
                return;
            }
            if (s.stopped) return;
            if (s.failed && !s.let_go)
                fail(s, "a failed stream is not let go");
            if (s.busy || s.posted || s.write_out)
                fail(s, "the stream is left busy");
            if (!s.failed && s.backlog != 0)
                fail(s, "a backlog is left");
        }

        void visit(const state& s) {
            if (!fault.empty()) return;
            if (!seen.insert(s.key()).second) return;
            ++states;
            invariants(s);
            bool any = false;
            auto go = [&](state t) {
                any = true;
                visit(t);
            };

            // the worker
            if (s.ph == phase::running) {
                state t = s;
                const uint64_t b = below(t);
                switch (r.wait_step(t.failed, t.backlog, b)) {
                    case on_wait::give_up:
                        t.ph = phase::gave_up;
                        break;
                    case on_wait::wait:
                        t.ph = phase::waiting;
                        t.seen = t.progress;
                        t.took_seen = t.took;
                        break;
                    case on_wait::go:
                        if (t.at < pieces.size()) {
                            if (t.backlog >= high_water) fail(t, "a worker queues at the high water");
                            const uint64_t n = pieces[t.at];
                            t.pending += n;
                            t.backlog += n;
                            t.queued += n;
                            if (r.posts_next(t.busy)) {
                                t.busy = true;
                                ++t.posted;
                            }
                            ++t.at;
                        } else {
                            t.ph = phase::done;
                            if (t.failed) fail(t, "the worker finished on a failed stream");
                            if (t.took != total()) fail(t, "the worker finished with bytes not taken");
                        }
                        break;
                }
                go(t);
            } else if (s.ph == phase::waiting) {
                // the cv's predicate, notified on every change under the lock
                if (moved(s, below(s))) {
                    state t = s;
                    t.ph = phase::running;
                    go(t);
                } else if (s.timeouts < lim.timeouts) {
                    state t = s;
                    if (t.took != t.took_seen) fail(t, "a wait times out with the client taking bytes");
                    ++t.timeouts;
                    t.failed = true;
                    ++t.net_errors;
                    ++t.shutdowns_posted;
                    t.ph = phase::gave_up;
                    go(t);
                }
            }

            // the server stops: the stream fails and the socket's thread stops
            if (!s.stopped && s.stops < lim.stops) {
                state t = s;
                ++t.stops;
                t.stopped = true;
                t.failed = true;
                go(t);
            }

            if (!s.stopped) {
                if (s.posted) {
                    state t = s;
                    --t.posted;
                    next(t);
                    go(t);
                }
                if (s.shutdowns_posted) {
                    state t = s;
                    --t.shutdowns_posted;
                    t.shut = true;
                    t.let_go = true;
                    go(t);
                }
                if (s.write_out) {
                    if (s.shut || s.gone) {
                        state t = s;
                        complete(t, true, 0);
                        go(t);
                    } else {
                        {
                            state t = s;
                            complete(t, false, 1);
                            go(t);
                        }
                        if (s.piece > 1) {
                            state t = s;
                            complete(t, false, s.piece);
                            go(t);
                        }
                        if (s.resets < lim.resets) {
                            state t = s;
                            ++t.resets;
                            t.gone = true;
                            complete(t, true, 0);
                            go(t);
                        }
                    }
                }
            }
            if (!any) terminal(s);
        }
    };

    // every worker and every world; empty if it all holds
    std::string all(const rules& r, size_t* states = nullptr) {
        std::vector<std::vector<uint64_t>> jobs;
        std::vector<uint64_t> job;
        std::function<void()> grow = [&]() {
            if (!job.empty()) jobs.push_back(job);
            if (job.size() == 3) return;
            for (uint64_t n = 1; n <= 3; ++n) {
                job.push_back(n);
                grow();
                job.pop_back();
            }
        };
        grow();
        size_t total = 0;
        for (const auto& j : jobs) {
            for (uint8_t t = 0; t <= 1; ++t)
                for (uint8_t rs = 0; rs <= 1; ++rs)
                    for (uint8_t st = 0; st <= 1; ++st) {
                        explorer e{r, j, {t, rs, st}, {}, {}, 0};
                        e.visit(state{});
                        total += e.states;
                        if (!e.fault.empty()) {
                            std::string what = "pieces";
                            for (auto p : j) what += " " + std::to_string(p);
                            what += " timeouts " + std::to_string(t) + " resets " + std::to_string(rs) +
                                    " stops " + std::to_string(st) + ": " + e.fault;
                            return what;
                        }
                    }
        }
        if (states) *states = total;
        return {};
    }
}

int main() {
    const rules now;

    std::printf("every worker against every order of writes, resets, timeouts and stops\n");
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
                {"a failed write does not let go, as before TODO 943", with([](rules& r) {
                    r.written = [](bool e, bool f, uint64_t b, uint64_t l) {
                        auto w = written(e, f, b, l);
                        w.let_go = false;
                        return w;
                    };
                })},
                {"a failed stream writes the rest, as before TODO 943", with([](rules& r) {
                    r.next = [](bool, bool all, bool empty) { return next(false, all, empty); };
                })},
                {"the worker ignores a failed stream", with([](rules& r) {
                    r.wait_step = [](bool, uint64_t b, uint64_t below) { return wait_step(false, b, below); };
                })},
                {"the worker goes at the limit", with([](rules& r) {
                    r.wait_step = [](bool f, uint64_t b, uint64_t below) { return wait_step(f, b, below + 1); };
                })},
                {"stream_next never posted", with([](rules& r) {
                    r.posts_next = [](bool) { return false; };
                })},
                {"stream_next posted while busy", with([](rules& r) {
                    r.posts_next = [](bool) { return true; };
                })},
                {"a half written buffer dropped", with([](rules& r) {
                    r.next = [](bool f, bool, bool empty) { return next(f, true, empty); };
                })},
                {"a write takes nothing off the backlog", with([](rules& r) {
                    r.written = [](bool e, bool f, uint64_t b, uint64_t l) {
                        auto w = written(e, f, b, l);
                        w.backlog = b;
                        return w;
                    };
                })},
                {"a write is not progress", with([](rules& r) {
                    r.written = [](bool e, bool f, uint64_t b, uint64_t l) {
                        auto w = written(e, f, b, l);
                        w.progress = false;
                        return w;
                    };
                })},
                {"every failed write a net error", with([](rules& r) {
                    r.written = [](bool e, bool f, uint64_t b, uint64_t l) {
                        auto w = written(e, f, b, l);
                        w.count_error = e;
                        return w;
                    };
                })},
                {"a failed write leaves the stream open", with([](rules& r) {
                    r.written = [](bool e, bool f, uint64_t b, uint64_t l) {
                        auto w = written(e, f, b, l);
                        w.failed = f;
                        return w;
                    };
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
