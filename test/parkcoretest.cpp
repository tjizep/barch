// The park core, every short sequence of events - TODO 920, 923.
//
// src/rpc/park_core.h holds what a parked connection decides. This runs those
// decisions against a small model of the socket, the read chain and a batch,
// through every order of events up to a few of each, and checks:
//   - a client that went away is not still parked;
//   - a client that is still there was not dropped;
//   - nothing pipelined behind the blocking command runs before its reply;
//   - nothing pipelined is left waiting once the chain is back;
//   - there is never more than one read out on the socket.
//
// A read in the model completes on whatever is in the socket, already there or
// new. That is what asio does: it arms with EPOLL_CTL_MOD, and the kernel reports
// what is already there. Modelling the arm as edge only hid a spin - TODO 921, 923.
//
// It also runs rules that are known to be wrong and checks that it catches them,
// so a model that checks nothing fails too:
//   - running what a park's read brought;
//   - a park that takes pipelined bytes for a disconnect (TODO 226);
//   - a park that stops reading after the first bytes (TODO 919);
//   - the chain starting a read beside the park's;
//   - kept bytes not marked pending, so a batch that ends never runs them;
//   - a park that stops reading once the parser is full. TCP then holds the FIN
//     behind the client's unsent data, so it never comes - TODO 924.
//
// A batch's writes have their own model, TODO 935: a client that resets before any
// write of a batch stops it there, and no call after the failed write runs. One of
// the wrong rules is the code before TODO 935: write_then went on whatever its
// write did.
//
// A client past rpc_query_buffer_limit is closed. That is the one drop of a client
// still there that is allowed, and the model counts it apart.
#include "rpc/park_core.h"

#include <cstdio>
#include <functional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

using namespace barch;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    struct rules {
        size_t most = 3;                // what a park may keep, rpc_query_buffer_limit
        bool stops_when_full = false;   // the old rule: a full parser stops the read
        bool runs_park_reads = false;   // a park's read runs what it brought
        bool drops_on_bytes = false;    // pipelined input read as a disconnect
        bool stops_on_bytes = false;    // no read again after the first bytes
        bool chain_reads_beside = false;// the chain starts its own read regardless
        bool forgets_pending = false;   // kept bytes not marked to run
    };

    struct state {
        // the session
        bool parked = true;
        bool batch = false;     // answered into a batch: the chain is down until it ends
        bool read_out = false;
        bool for_park = false;
        bool pending = false;   // parse_pending
        size_t buf = 0;         // requests kept in the parser
        bool dropped = false;
        bool over = false;      // closed for passing the limit
        // the client and its socket
        int sock = 0;           // requests sent and not read
        bool fin = false;
        // what is left to happen
        int sends = 2, answers = 1;

        auto key() const {
            return std::make_tuple(parked, batch, read_out, for_park, pending, buf, dropped, over,
                                   sock, fin, sends, answers);
        }
        bool operator<(const state& o) const { return key() < o.key(); }
        bool on_its_way() const { return read_out && (sock > 0 || fin); }
    };

    struct checker {
        rules r;
        std::set<state> seen;
        std::vector<std::string> trace;
        std::string found;      // the first violation, with how it got there

        void fail(const char* why) {
            if (!found.empty()) return;
            found = why;
            for (auto& s : trace) found += "\n    " + s;
        }
        // watch_for_disconnect
        void watch(state& s) {
            if (!park::watch_now(s.parked, s.read_out))
                return;
            if (r.stops_when_full && s.buf >= r.most)
                return;
            s.read_out = true;
            s.for_park = true;
        }
        // do_read(), from the chain
        void chain_read(state& s) {
            if (r.chain_reads_beside || park::chain_reads(s.read_out)) {
                if (s.read_out) fail("two reads at once");
                s.read_out = true;
            }
            s.for_park = false;
        }
        // consume_available on what is in the parser
        void run_kept(state& s) {
            if (s.parked || s.batch) fail("a command ran ahead of the blocked reply");
            s.buf = 0;
        }
        // continue_reading
        void resume(state& s) {
            if (s.pending) {
                s.pending = false;
                run_kept(s);
            }
            chain_read(s);
        }
        void read_completes(state& s) {
            const bool fp = s.for_park;
            s.read_out = false;
            s.for_park = false;
            if (s.sock == 0) {
                // the end of the stream: the read handler's error path
                if (s.parked) {
                    s.parked = false;
                    s.dropped = true;
                }
                return;
            }
            --s.sock;
            ++s.buf;
            auto a = park::on_arrival(fp, s.parked, s.buf, r.most);
            if (r.runs_park_reads) a = park::arrived::run;
            if (r.drops_on_bytes && fp && s.parked) {
                s.parked = false;
                s.dropped = true;
                return;
            }
            if (r.stops_when_full && a == park::arrived::drop)
                a = park::arrived::hold;
            switch (a) {
                case park::arrived::drop:
                    s.parked = false;
                    s.dropped = true;
                    s.over = true;
                    s.sock = 0;
                    s.fin = true;   // the server closed it: nothing more comes
                    return;
                case park::arrived::keep:
                    if (!r.forgets_pending) s.pending = true;
                    if (!r.stops_on_bytes) watch(s);
                    return;
                case park::arrived::hold:
                    if (!r.forgets_pending) s.pending = true;
                    return;
                case park::arrived::run:
                    run_kept(s);
                    chain_read(s);
                    return;
            }
        }

        void run(state s) {
            if (!found.empty()) return;
            if (s.dropped && !s.fin && !s.over)
                return fail("a client still there was dropped");
            if (s.over && s.buf <= r.most)
                return fail("a client within the limit was closed for it");
            if (!seen.insert(s).second)
                return;
            auto step = [&](const char* name, auto&& change) {
                state n = s;
                trace.emplace_back(name);
                change(n);
                run(n);
                trace.pop_back();
            };
            if (s.on_its_way())
                step(s.sock > 0 ? "a read brings a request" : "a read ends", [&](state& n) { read_completes(n); });
            if (!s.fin && s.sends > 0)
                step("client sends", [&](state& n) { ++n.sock; --n.sends; });
            if (!s.fin)
                step("client closes", [&](state& n) { n.fin = true; });
            if (s.parked && s.answers > 0) {
                // resume_after_blocks
                step("the block answers", [&](state& n) {
                    n.parked = false; --n.answers; n.pending = true; resume(n);
                });
                step("the block answers into a batch", [&](state& n) {
                    n.parked = false; --n.answers; n.batch = true;
                });
            }
            if (s.batch)
                step("the batch ends", [&](state& n) { n.batch = false; resume(n); });
            /*
             * Nothing is on its way to the session. Whatever else may still happen,
             * it must not need it to let go, or to run what it was sent.
             */
            if (s.on_its_way() || !found.empty())
                return;
            if (s.fin && s.parked)
                return fail("a client that went away is still parked");
            if (!s.parked && !s.batch && !s.dropped && (s.buf > 0 || (s.sock > 0 && !s.read_out)))
                return fail("pipelined commands were left waiting");
        }
    };

    std::string explore(const rules& r) {
        checker c{r, {}, {}, {}};
        state s;
        c.watch(s);     // add_caller_blocks
        c.run(s);
        return c.found;
    }

    void expect_clean(const char* what, const rules& r) {
        auto f = explore(r);
        check(f.empty(), what);
        if (!f.empty()) std::printf("  %s\n", f.c_str());
    }

    void expect_caught(const char* what, const rules& r, const char* why) {
        auto f = explore(r);
        check(f.rfind(why, 0) == 0, what);
        std::printf("  %s\n", f.empty() ? "(nothing found)" : f.c_str());
    }
}

/*
 * The whole read chain - TODO 930. The model above starts parked and has no back
 * pressure. This one starts reading and runs the client's commands through every
 * mode: reading, paused while replies drain, parked from a read, in a batch, and
 * parked part way through a batch. A command is p (answered at once), b (parks
 * until answered) or a (runs on the pool, so it and all behind it are a batch).
 * Replies count in units, and the high water mark is HIGH of them.
 *
 * Checked in every state: the six values make a mode that can happen. Checked
 * once nothing is on its way to the session:
 *   - a client that went away is not still parked;
 *   - a parked client has had every reply ahead of the block;
 *   - a session not parked has a read out, and every command has its reply;
 * and along the way: nothing runs while parked or in a batch, there are never two
 * reads or two writes out, and replies never pile up past the mark.
 *
 * The wrong rules it must catch:
 *   - replies ahead of a park held until it answers (the code before TODO 930);
 *   - a chain that reads on past the high water mark;
 *   - a pause that never resumes;
 *   - kept requests read past instead of run;
 *   - an answer in a batch that forgets the rest of the batch;
 *   - a batch that leaves the chain reading beside it.
 */
namespace {
    constexpr size_t HIGH = 2;

    struct chain_rules {
        bool holds_before_park = false;
        bool ignores_pause = false;
        bool never_resumes = false;
        bool ignores_pending = false;
        bool forgets_batch = false;
        bool batch_reads_beside = false;
    };

    int bits(const park::chain& c) {
        return c.paused | c.read_out << 1 | c.for_park << 2 | c.parked << 3 | c.in_batch << 4
               | c.batch_waiting << 5;
    }

    enum waiter : char { start_batch = 's', after_blocks = 'r' };

    struct chain_state {
        // the client and its socket
        std::string sock;       // sent and not read
        bool fin = false;
        int sends = 5;
        int made = 0;           // commands sent
        int wire = 0;           // replies on the wire
        // the session
        bool read_out = true;   // the first do_read
        bool for_park = false;
        std::string buf;        // kept in the parser
        bool paused = false;
        bool pending = false;   // parse_pending
        bool posted = false;    // do_read(true)'s posted parse
        bool parked = false;    // caller.has_blocks()
        bool in_batch = false;
        std::string batch;      // calls left
        size_t carry = 0;       // sync replies carried into the batch's first call
        bool go = false;        // the batch's next call may run
        bool waiting = false;   // pending_batch
        size_t held = 0;        // replies in `stream`, not yet sent
        size_t backlog = 0;     // replies sent and not yet written
        std::string after;      // after_sent
        bool closed = false;

        auto key() const {
            return std::make_tuple(sock, fin, sends, made, wire, read_out, for_park, buf, paused,
                                   pending, posted, parked, in_batch, batch, carry, go, waiting,
                                   held, backlog, after, closed);
        }
        bool operator<(const chain_state& o) const { return key() < o.key(); }
        park::chain chain() const {
            return {paused, read_out, for_park, parked, in_batch, waiting};
        }
    };

    struct chain_checker {
        chain_rules r;
        std::set<chain_state> seen;
        std::set<int> reached;  // the combinations of the six seen
        std::vector<std::string> trace;
        std::string found;

        void fail(const char* why) {
            if (!found.empty()) return;
            found = why;
            for (auto& t : trace) found += "\n    " + t;
        }
        void send(chain_state& s) {
            s.backlog += s.held;
            s.held = 0;
        }
        void when_sent(chain_state& s, waiter w) {
            if (s.backlog == 0) return fire(s, w);
            s.after += (char) w;
        }
        void fire(chain_state& s, waiter w) {
            if (w == start_batch) {
                s.go = true;
                return;
            }
            // resume_after_blocks
            if (park::after_block(s.waiting && !r.forgets_batch) == park::after_answer::rest_of_batch) {
                s.waiting = false;
                s.go = true;
                return;
            }
            s.pending = true;
            continue_reading(s);
        }
        void do_read(chain_state& s) {
            if (park::chain_reads(s.read_out)) {
                if (s.read_out) fail("two reads at once");
                s.read_out = true;
            }
            s.for_park = false;
        }
        void watch(chain_state& s) {
            if (!park::watch_now(s.parked, s.read_out))
                return;
            s.read_out = true;
            s.for_park = true;
        }
        void continue_reading(chain_state& s) {
            auto n = park::continue_reading(s.backlog, HIGH, s.pending);
            if (r.ignores_pause && n == park::next::pause)
                n = s.pending ? park::next::run_kept : park::next::read;
            if (r.ignores_pending && n == park::next::run_kept)
                n = park::next::read;
            switch (n) {
                case park::next::pause:
                    s.paused = true;
                    return;
                case park::next::run_kept:
                    s.pending = false;
                    s.posted = true;
                    return;
                case park::next::read:
                    do_read(s);
                    return;
            }
        }
        void resume_if_drained(chain_state& s) {
            if (r.never_resumes || !park::resumes(s.paused, s.backlog, HIGH))
                return;
            s.paused = false;
            continue_reading(s);
        }
        // consume_available. true if parked or a batch started
        bool consume(chain_state& s) {
            if (s.parked || s.in_batch) fail("a command ran ahead of a reply it waits for");
            if (s.held) fail("replies were cleared before they went out");
            std::string calls;
            while (!s.buf.empty()) {
                const char c = s.buf[0];
                s.buf.erase(0, 1);
                if (!calls.empty() || c == 'a')
                    calls += c;
                else if (c == 'p')
                    ++s.held;
                else {
                    s.parked = true;
                    break;
                }
                if (!s.buf.empty() && s.held + s.backlog >= HIGH) {
                    s.pending = true;
                    break;
                }
            }
            if (!calls.empty()) {
                s.in_batch = true;
                s.batch = calls;
                s.carry = s.held;
                s.held = 0;
                when_sent(s, start_batch);
                if (r.batch_reads_beside) continue_reading(s);
                return true;
            }
            if (s.parked) {
                if (!r.holds_before_park) send(s);
                watch(s);
                return true;
            }
            return false;
        }
        // on_read's run case
        void run_path(chain_state& s) {
            if (consume(s)) return;
            send(s);
            continue_reading(s);
        }
        void read_completes(chain_state& s) {
            const bool fp = s.for_park;
            s.read_out = false;
            s.for_park = false;
            if (s.sock.empty()) {
                // the end of the stream: erase_blocks, and the session goes
                s.parked = false;
                s.closed = true;
                return;
            }
            s.buf += s.sock[0];
            s.sock.erase(0, 1);
            switch (park::on_arrival(fp, s.parked, s.buf.size(), 8)) {
                case park::arrived::keep:
                    s.pending = true;
                    watch(s);
                    return;
                case park::arrived::hold:
                    s.pending = true;
                    return;
                case park::arrived::drop:
                    fail("a client within the limit was closed for it");
                    return;
                case park::arrived::run:
                    run_path(s);
                    return;
            }
        }
        void drains(chain_state& s) {
            --s.backlog;
            ++s.wire;
            if (s.backlog == 0) {
                auto waiting = s.after;
                s.after.clear();
                for (char w : waiting) fire(s, (waiter) w);
            }
            resume_if_drained(s);
        }
        // a call of the batch runs on the pool and writes its reply with write_then
        void batch_call(chain_state& s) {
            if (s.backlog) fail("two writes at once");
            s.wire += (int) s.carry;
            s.carry = 0;
            const char c = s.batch[0];
            s.batch.erase(0, 1);
            if (c == 'b') {
                // suspend_for_blocks
                s.go = false;
                s.waiting = true;
                s.parked = true;
                watch(s);
                return;
            }
            ++s.wire;
        }
        void answer(chain_state& s) {
            // write_result, erase_blocks, send, when_sent(resume_after_blocks)
            s.parked = false;
            ++s.held;
            send(s);
            when_sent(s, after_blocks);
        }

        void run(chain_state s) {
            if (!found.empty()) return;
            if (park::mode_of(s.chain()) == park::mode::impossible)
                return fail("a mode that can't happen");
            reached.insert(bits(s.chain()));
            if (s.backlog > HIGH)
                return fail("replies piled up past the high water mark");
            if (!seen.insert(s).second)
                return;
            if (s.closed)
                return;
            auto step = [&](const char* name, auto&& change) {
                chain_state n = s;
                trace.emplace_back(name);
                change(n);
                run(n);
                trace.pop_back();
            };
            bool busy = false;
            if (s.read_out && (!s.sock.empty() || s.fin)) {
                busy = true;
                step(s.sock.empty() ? "a read ends" : "a read brings a command",
                     [&](chain_state& n) { read_completes(n); });
            }
            if (s.backlog) {
                busy = true;
                step("a write drains", [&](chain_state& n) { drains(n); });
            }
            if (s.posted) {
                busy = true;
                step("the posted parse runs", [&](chain_state& n) { n.posted = false; run_path(n); });
            }
            if (s.in_batch && s.go && !s.waiting) {
                busy = true;
                if (!s.batch.empty())
                    step("a batch call runs", [&](chain_state& n) { batch_call(n); });
                else
                    step("the batch ends", [&](chain_state& n) {
                        n.in_batch = false; n.go = false; continue_reading(n);
                    });
            }
            if (s.parked)
                step("the block answers", [&](chain_state& n) { answer(n); });
            if (!s.fin && s.sends > 0)
                for (char c : std::string("pba"))
                    step(c == 'p' ? "client sends p" : c == 'b' ? "client sends b" : "client sends a",
                         [&](chain_state& n) { n.sock += c; --n.sends; ++n.made; });
            if (!s.fin)
                step("client closes", [&](chain_state& n) { n.fin = true; });
            if (busy || !found.empty())
                return;
            // nothing is on its way to the session
            if (s.fin && s.parked)
                return fail("a client that went away is still parked");
            if (s.parked) {
                if (s.held) fail("replies ahead of a block were held");
                return;
            }
            if (s.waiting)
                return;
            if (!s.read_out)
                return fail("the chain stopped");
            if (s.wire != s.made && s.sock.empty())
                return fail("a reply never went out");
        }
    };

    std::string explore_chain(const chain_rules& r, size_t* reached = nullptr) {
        chain_checker c{r, {}, {}, {}, {}};
        c.run(chain_state{});
        if (reached) *reached = c.reached.size();
        return c.found;
    }

    void chain_clean(const char* what, const chain_rules& r) {
        auto f = explore_chain(r);
        check(f.empty(), what);
        if (!f.empty()) std::printf("  %s\n", f.c_str());
    }

    void chain_caught(const char* what, const chain_rules& r, const char* why) {
        auto f = explore_chain(r);
        check(f.rfind(why, 0) == 0, what);
        std::printf("  %s\n", f.empty() ? "(nothing found)" : f.c_str());
    }
}

/*
 * A batch and the connection's state - TODO 931. Each call in a batch runs on a copy
 * of the session's caller made when the batch is read, and the copy is dropped after.
 * The model runs every pipeline of up to five commands two ways: one at a time on
 * one connection, which is what the client asked for, and the way the session does
 * it, with the batch rules from park_core.h. The replies and the state the
 * connection ends in must be the same.
 */
namespace {
    enum class bop { get, keys, use_a, use_b, multi, exec };
    const char* const bop_names[] = {"GET", "KEYS", "USE", "USE", "MULTI", "EXEC"};
    const char* const bop_shows[] = {"GET", "KEYS", "USE a", "USE b", "MULTI", "EXEC"};

    struct conn_state {
        int space = 0;
        bool buffering = false;
        std::vector<bop> queue{};
        bool operator==(const conn_state& o) const {
            return space == o.space && buffering == o.buffering && queue == o.queue;
        }
    };

    // a command on a connection, as one at a time does it
    void apply(conn_state& c, bop o, std::string& out) {
        if (c.buffering && o != bop::exec) {
            c.queue.push_back(o);
            out += "Q ";
            return;
        }
        switch (o) {
            case bop::multi: c.buffering = true; out += "ok "; return;
            case bop::exec: {
                if (!c.buffering) { out += "err "; return; }
                c.buffering = false;
                auto q = std::move(c.queue);
                c.queue.clear();
                out += "[ ";
                for (auto e : q) apply(c, e, out);
                out += "] ";
                return;
            }
            case bop::use_a: c.space = 1; out += "ok "; return;
            case bop::use_b: c.space = 2; out += "ok "; return;
            default: out += "s" + std::to_string(c.space) + " "; return;
        }
    }

    struct batch_rules {
        bool no_hold = false;           // the old code: nothing waits for the batch
        bool async_under_multi = false; // the old code: KEYS under MULTI goes to a copy
        bool use_only = false;          // a list of connection commands with USE alone
    };

    // the session: what joins a batch runs later, on a copy made now
    void run_session(const batch_rules& r, const std::vector<bop>& seq, conn_state& s, std::string& out) {
        size_t i = 0;
        bool have_held = false;
        bop held = bop::get;
        while (have_held || i < seq.size()) {
            std::vector<std::pair<bop, conn_state>> batch;
            while (have_held || i < seq.size()) {
                bop o = have_held ? held : seq[i++];
                have_held = false;
                const std::string name = bop_names[(int) o];
                const bool changes = r.use_only ? name == "USE" : park::changes_connection(name);
                if (!r.no_hold && park::holds(!batch.empty(), changes)) {
                    held = o;
                    have_held = true;
                    break;
                }
                const bool asynch = o == bop::keys;
                const bool joins = r.async_under_multi ? (!batch.empty() || asynch)
                                                       : park::joins_batch(!batch.empty(), asynch, s.buffering);
                if (joins)
                    batch.emplace_back(o, s);
                else
                    apply(s, o, out);
            }
            for (auto& [o, copy] : batch)
                apply(copy, o, out);
        }
    }

    std::string explore_batches(const batch_rules& r, size_t* runs = nullptr) {
        std::vector<bop> seq;
        size_t n = 0;
        std::string found;
        auto show = [&]() {
            std::string w;
            for (auto o : seq) w += std::string(w.empty() ? "" : ", ") + bop_shows[(int) o];
            return w;
        };
        size_t length = 0;
        // by length, so what it finds is the shortest pipeline that shows it
        std::function<void()> go = [&]() {
            if (!found.empty()) return;
            if (seq.size() == length) {
                ++n;
                conn_state want, got;
                std::string want_out, got_out;
                for (auto o : seq) apply(want, o, want_out);
                run_session(r, seq, got, got_out);
                if (got_out != want_out) {
                    found = "a reply differs from one at a time: " + show() + " gives " + got_out
                          + "not " + want_out;
                    return;
                }
                if (!(got == want)) {
                    found = "the connection ends in another state: " + show();
                    return;
                }
                return;
            }
            for (int o = 0; o < 6; ++o) {
                seq.push_back((bop) o);
                go();
                seq.pop_back();
            }
        };
        for (length = 1; length <= 5 && found.empty(); ++length)
            go();
        if (runs) *runs = n;
        return found;
    }

    void batch_clean(const char* what, const batch_rules& r) {
        auto f = explore_batches(r);
        check(f.empty(), what);
        if (!f.empty()) std::printf("  %s\n", f.c_str());
    }

    void batch_caught(const char* what, const batch_rules& r, const char* why) {
        auto f = explore_batches(r);
        check(f.rfind(why, 0) == 0, what);
        std::printf("  %s\n", f.empty() ? "(nothing found)" : f.c_str());
    }
}

namespace {
    /*
     * The end of a session - TODO 932. A client sends requests and ends in one of four
     * ways: a FIN, a reset, and over TLS a bad record or a close_notify, after which
     * its TCP stays open. The session reads, writes replies, and when its read ends
     * follows park::on_read_end. The collector follows park::collects, with a peek
     * that reads 0 once the peer's FIN or reset has been read, or the session shut its
     * own socket. Every order of client moves and session steps is tried, breadth
     * first, so the first fault found is the shortest.
     *
     * The collector may run in any state, not only at the end - TODO 933. A session
     * with a read, a write or a pending shut is held by its handler. Dropped from the
     * list while held, stop() can no longer reach it.
     */
    enum class move { req, fin, rst, bad, notify, read, write };
    const char* move_names[] = {"req", "FIN", "reset", "bad record", "close_notify",
                                "the session reads", "a reply goes out"};

    struct end_rules {
        bool never_shut{false};     // the code before TODO 932
        bool open_on_eof{false};    // shut on an error only
        bool shut_at_once{false};   // not after what is queued
        bool always_collect{false}; // the collector drops every session
        bool ignore_held{false};    // the rule before TODO 933: a peek of 0 drops it
    };

    struct end_state {
        bool tls{false};
        std::vector<char> inq;      // unread: 'q' a request, 'b' a bad record, 'n' close_notify
        bool fin{false}, rst{false}, client_done{false};
        int sent{0};                // requests the client sent
        int owed{0};                // requests it is owed a reply for
        int got{0};                 // replies it got
        bool reading{true};
        int write_out{0};
        bool shut_pending{false}, shut{false}, let_go{false};
        int client_moves{0};
        std::vector<move> path;
    };

    void end_read(end_state& s, const end_rules& r, bool eof) {
        s.reading = false;
        auto e = park::on_read_end(eof, false);
        if (r.never_shut || (r.open_on_eof && eof))
            e.shut = false;
        if (!e.shut)
            return;
        if (s.write_out > 0 && !r.shut_at_once) {
            s.shut_pending = true;
            return;
        }
        s.write_out = 0;    // what was not out is lost
        s.shut = s.let_go = true;
    }

    std::vector<move> moves(const end_state& s) {
        std::vector<move> m;
        const bool can = !s.client_done && s.client_moves < 4;
        if (can) {
            m.push_back(move::req);
            m.push_back(move::fin);
            m.push_back(move::rst);
            if (s.tls) {
                m.push_back(move::bad);
                m.push_back(move::notify);
            }
        }
        if (s.reading && (!s.inq.empty() || s.fin || s.rst))
            m.push_back(move::read);
        if (s.write_out > 0)
            m.push_back(move::write);
        return m;
    }

    end_state step(end_state s, move m, const end_rules& r) {
        s.path.push_back(m);
        switch (m) {
            case move::req:
                ++s.client_moves; ++s.sent; ++s.owed;
                s.inq.push_back('q');
                break;
            case move::fin:
                ++s.client_moves; s.fin = s.client_done = true;
                break;
            case move::rst:
                ++s.client_moves; s.rst = s.client_done = true;
                s.inq.clear();      // a reset drops what was unread
                s.owed = 0;         // and nothing more reaches the client
                break;
            case move::bad:
                ++s.client_moves; s.client_done = true;
                s.inq.push_back('b');
                break;
            case move::notify:
                ++s.client_moves; s.client_done = true;
                s.inq.push_back('n');
                break;
            case move::read:
                if (s.inq.empty()) {
                    end_read(s, r, !s.rst);
                    break;
                }
                {
                    const char c = s.inq.front();
                    s.inq.erase(s.inq.begin());
                    if (c == 'q')
                        ++s.write_out;
                    else
                        end_read(s, r, c == 'n');   // asio reads a close_notify as the end
                }
                break;
            case move::write:
                --s.write_out;
                if (!s.rst) ++s.got;
                if (s.write_out == 0 && s.shut_pending) {
                    s.shut_pending = false;
                    s.shut = s.let_go = true;
                }
                break;
        }
        return s;
    }

    std::string show_path(const end_state& s) {
        std::string w = s.tls ? "TLS: " : "TCP: ";
        for (size_t i = 0; i < s.path.size(); ++i)
            w += std::string(i ? ", " : "") + move_names[(int) s.path[i]];
        return w;
    }

    bool end_alive(const end_state& s) {
        return s.reading || s.write_out > 0 || s.shut_pending;
    }

    bool end_collected(const end_state& s, const end_rules& r) {
        const long peek = (s.shut || ((s.fin || s.rst) && s.inq.empty())) ? 0 : -1;
        const bool held = end_alive(s) && !r.ignore_held;
        return r.always_collect || park::collects(s.let_go, held, peek);
    }

    /** any state: the collector may run here - TODO 933 */
    std::string drop_fault(const end_state& s, const end_rules& r) {
        if (end_alive(s) && end_collected(s, r))
            return "a running session is dropped from the list: " + show_path(s);
        return {};
    }

    /** a state with no session step left: the client may stop here */
    std::string end_fault(const end_state& s, const end_rules& r) {
        const bool alive = end_alive(s);
        const bool collected = end_collected(s, r);
        if (!alive && !collected)
            return "a session nothing will run again is kept: " + show_path(s);
        if (s.reading && !s.fin && !s.rst && s.inq.empty() && collected)
            return "a session its client still uses is dropped: " + show_path(s);
        // every request read before the end is answered, unless the client reset
        int answerable = 0;
        for (auto m : s.path) {
            if (m == move::req) ++answerable;
            if (m == move::bad || m == move::notify) break;
        }
        if (!s.rst && s.got != answerable)
            return "a reply is lost: " + show_path(s) + " got " + std::to_string(s.got)
                 + " of " + std::to_string(answerable);
        return {};
    }

    std::string explore_ends(const end_rules& r, size_t* states = nullptr) {
        size_t n = 0;
        for (bool tls : {false, true}) {
            std::vector<end_state> level{end_state{}};
            level[0].tls = tls;
            while (!level.empty()) {
                std::vector<end_state> next;
                for (auto& s : level) {
                    ++n;
                    auto m = moves(s);
                    bool session_step = false;
                    for (auto x : m)
                        session_step = session_step || x == move::read || x == move::write;
                    auto f = session_step ? std::string{} : end_fault(s, r);
                    if (f.empty())
                        f = drop_fault(s, r);
                    if (!f.empty()) {
                        if (states) *states = n;
                        return f;
                    }
                    for (auto x : m)
                        next.push_back(step(s, x, r));
                }
                level = std::move(next);
            }
        }
        if (states) *states = n;
        return {};
    }

    void end_clean(const char* what, const end_rules& r) {
        auto f = explore_ends(r);
        check(f.empty(), what);
        if (!f.empty()) std::printf("  %s\n", f.c_str());
    }

    void end_caught(const char* what, const end_rules& r, const char* why) {
        auto f = explore_ends(r);
        check(f.rfind(why, 0) == 0, what);
        std::printf("  %s\n", f.empty() ? "(nothing found)" : f.c_str());
    }
}

namespace {
    /*
     * A batch's writes - TODO 935. A batch of one to four calls, each plain or one
     * that blocks. A worker writes each call's reply in four parts, the last through
     * write_then. The client resets before any one of those writes, or never, and
     * every write from then on fails. Checked:
     *   - no call runs after a write failed;
     *   - a failed write lets the client go, and nothing parks or runs after it;
     *   - a call parks only once its reply is written;
     *   - with no reset, every call runs up to the first block, or to the end.
     */
    using write_rule = std::function<park::after_write(bool, bool)>;

    struct write_rules {
        write_rule part = park::batch_after_write;      // the three parts before the reply
        write_rule reply = park::batch_after_write;     // write_then's
    };

    constexpr int write_parts = 4;

    // empty if it all holds
    std::string one_batch(const write_rules& r, const std::vector<bool>& blocks, int reset_at) {
        int writes = 0;
        bool failed = false;
        size_t ran = 0;
        bool parked = false, let_go = false;
        for (size_t i = 0; i < blocks.size() && !parked && !let_go; ++i) {
            if (failed)
                return "a call ran after a write failed";
            ++ran;
            for (int part = 0; part < write_parts; ++part) {
                const bool written = reset_at < 0 || writes < reset_at;
                ++writes;
                if (!written)
                    failed = true;
                const bool last = part == write_parts - 1;
                const auto next = last ? r.reply(written, blocks[i]) : r.part(written, false);
                if (!written && next != park::after_write::stop)
                    return next == park::after_write::park ? "a call parked after its write failed"
                                                           : "the batch went on after a write failed";
                if (next == park::after_write::stop) {
                    if (written)
                        return "a batch stopped with its client still there";
                    let_go = true;
                    break;
                }
                if (next == park::after_write::park) {
                    if (!last)
                        return "a call parked before its reply was written";
                    parked = true;
                    break;
                }
                if (last && blocks[i])
                    return "a call that blocked went on without parking";
            }
        }
        if (failed && !let_go)
            return "a client whose write failed was not let go";
        if (reset_at < 0) {
            size_t want = blocks.size();
            for (size_t i = 0; i < blocks.size(); ++i)
                if (blocks[i]) {
                    want = i + 1;
                    break;
                }
            if (ran != want)
                return "a call was lost with the client still there";
        }
        return "";
    }

    // every batch of one to four calls, against every reset point
    std::string all_writes(const write_rules& r, size_t* runs = nullptr) {
        size_t n = 0;
        for (size_t calls = 1; calls <= 4; ++calls)
            for (unsigned mask = 0; mask < (1u << calls); ++mask) {
                std::vector<bool> blocks;
                for (size_t i = 0; i < calls; ++i)
                    blocks.push_back(mask >> i & 1);
                for (int reset_at = -1; reset_at <= (int) calls * write_parts; ++reset_at) {
                    ++n;
                    auto got = one_batch(r, blocks, reset_at);
                    if (!got.empty())
                        return got;
                }
            }
        if (runs)
            *runs = n;
        return "";
    }
}

int main() {
    std::printf("the timer\n");
    {
        auto p = park::plan_timer(0, false, 0, 1000);
        check(!p.armed && p.deadline == 0, "a timeout of 0 waits for ever");
        p = park::plan_timer(~0ull, false, 0, 1000);
        check(!p.armed && p.deadline == 0, "a timeout past int64 waits for ever");
        p = park::plan_timer(500, false, 0, 1000);
        check(p.armed && p.wait_ms == 500 && p.deadline == 1500, "a first park waits its whole time");
        p = park::plan_timer(500, true, 1500, 1200);
        check(p.armed && p.wait_ms == 300 && p.deadline == 1500,
              "a park again keeps the time it had left - TODO 879");
        p = park::plan_timer(500, true, 1500, 1600);
        check(p.armed && p.wait_ms == 0 && p.deadline == 1500, "a deadline gone stays gone");
        p = park::plan_timer(500, false, 1500, 1600);
        check(p.armed && p.wait_ms == 500 && p.deadline == 2100, "a new park starts afresh");
    }

    std::printf("wakes and the timer meeting - TODO 878\n");
    {
        check(park::still_parked(3, 3, true), "the same park, with blocks, is still parked");
        check(!park::still_parked(3, 4, true), "a park again makes an old handler stale");
        check(!park::still_parked(3, 3, false), "an answered park is gone");
        check(park::timer_answers(false, true), "the timer answers a live park");
        check(!park::timer_answers(true, true), "a cancelled timer does nothing");
        check(!park::timer_answers(false, false), "the timer does nothing once answered");
        check(park::on_wake(false, 0, 10) == park::wake::pass_on, "a wake for a gone park passes the turn on");
        check(park::on_wake(true, 5, 10) == park::wake::time_out, "a wake after the deadline is a timeout");
        check(park::on_wake(true, 10, 10) == park::wake::time_out, "a wake at the deadline is a timeout");
        check(park::on_wake(true, 0, 10) == park::wake::run, "a wake with no deadline runs the waiter");
        check(park::on_wake(true, 20, 10) == park::wake::run, "a wake before the deadline runs the waiter");
    }

    std::printf("the park's read, every order of events\n");
    rules now;
    expect_clean("as now", now);

    rules runs = now;
    runs.runs_park_reads = true;
    expect_caught("catches running what a park's read brought", runs,
                  "a command ran ahead of the blocked reply");
    rules drops = now;
    drops.drops_on_bytes = true;
    expect_caught("catches a park that drops on pipelined bytes - TODO 226", drops,
                  "a client still there was dropped");
    rules stops = now;
    stops.stops_on_bytes = true;
    expect_caught("catches a park that stops reading on bytes - TODO 919", stops,
                  "a client that went away is still parked");
    rules beside = now;
    beside.chain_reads_beside = true;
    expect_caught("catches the chain reading beside a park's read", beside,
                  "two reads at once");
    rules forgets = now;
    forgets.forgets_pending = true;
    expect_caught("catches kept bytes not marked pending", forgets,
                  "pipelined commands were left waiting");
    rules small = now;
    small.most = 1;
    expect_clean("a client past a small limit is closed, not left parked - TODO 924", small);
    rules full = small;
    full.stops_when_full = true;
    expect_caught("catches a full parser that stops the read - TODO 924", full,
                  "a client that went away is still parked");

    std::printf("the read chain's choices - TODO 930\n");
    {
        check(park::continue_reading(2, 2, false) == park::next::pause, "replies at the mark pause the chain");
        check(park::continue_reading(2, 2, true) == park::next::pause, "and kept requests wait for them");
        check(park::continue_reading(1, 2, true) == park::next::run_kept, "kept requests run before a read");
        check(park::continue_reading(1, 2, false) == park::next::read, "else the chain reads");
        check(park::resumes(true, 0, 2) && !park::resumes(true, 1, 2), "a pause ends below half the mark");
        check(!park::resumes(false, 0, 2), "a chain not paused is not resumed");
        check(park::after_block(true) == park::after_answer::rest_of_batch, "an answer in a batch runs the rest");
        check(park::after_block(false) == park::after_answer::run_kept, "else what was kept runs");
    }
    std::printf("the modes - TODO 930\n");
    {
        // every combination of the six values, and the ones that can happen
        int possible = 0;
        for (int bits = 0; bits < 64; ++bits) {
            park::chain c{(bits & 1) != 0, (bits & 2) != 0, (bits & 4) != 0,
                          (bits & 8) != 0, (bits & 16) != 0, (bits & 32) != 0};
            if (park::mode_of(c) != park::mode::impossible) ++possible;
        }
        check(possible == 11, "11 of the 64 combinations can happen");
        check(park::mode_of({}) == park::mode::reading, "nothing set is reading");
        check(park::mode_of({false, true, false, false, false, false}) == park::mode::reading, "with a read out too");
        check(park::mode_of({true, false, false, false, false, false}) == park::mode::paused, "paused");
        check(park::mode_of({true, true, true, false, false, false}) == park::mode::paused,
              "paused, with an answered park's read still out");
        check(park::mode_of({false, true, true, true, false, false}) == park::mode::parked, "parked");
        check(park::mode_of({false, false, false, true, false, false}) == park::mode::impossible,
              "parked with no read out can't happen");
        check(park::mode_of({false, true, false, true, false, false}) == park::mode::impossible,
              "parked with the chain's own read out can't happen");
        check(park::mode_of({true, true, false, false, false, false}) == park::mode::impossible,
              "paused with the chain's own read out can't happen");
        check(park::mode_of({false, true, false, false, true, false}) == park::mode::impossible,
              "a batch with the chain's own read out can't happen");
        check(park::mode_of({true, true, true, true, false, false}) == park::mode::impossible,
              "paused while parked can't happen");
        check(park::mode_of({false, false, false, false, true, false}) == park::mode::batch, "in a batch");
        check(park::mode_of({false, true, true, true, true, true}) == park::mode::batch_parked,
              "parked part way through a batch");
        check(park::mode_of({false, true, true, true, true, false}) == park::mode::impossible,
              "parked in a batch that isn't waiting can't happen");
        check(park::mode_of({false, false, false, false, false, true}) == park::mode::impossible,
              "a batch waiting with no batch can't happen");
    }

    std::printf("the whole read chain, every order of events - TODO 930\n");
    chain_rules cnow;
    chain_clean("as now", cnow);
    size_t reached = 0;
    explore_chain(cnow, &reached);
    // so the rules in mode_of are as tight as the chain: none allows what never happens
    check(reached == 11, "and it reaches all 11 combinations mode_of allows");
    chain_rules held = cnow;
    held.holds_before_park = true;
    chain_caught("catches replies held behind a park (the old code)", held,
                 "replies ahead of a block were held");
    chain_rules nopause = cnow;
    nopause.ignores_pause = true;
    chain_caught("catches a chain that reads past the high water mark", nopause,
                 "replies piled up past the high water mark");
    chain_rules stuck = cnow;
    stuck.never_resumes = true;
    chain_caught("catches a pause that never resumes", stuck, "the chain stopped");
    chain_rules skips = cnow;
    skips.ignores_pending = true;
    chain_caught("catches kept requests read past", skips, "a reply never went out");
    chain_rules forgets_batch = cnow;
    forgets_batch.forgets_batch = true;
    chain_caught("catches an answer that forgets the rest of its batch", forgets_batch,
                 "a command ran ahead of a reply it waits for");
    chain_rules beside_batch = cnow;
    beside_batch.batch_reads_beside = true;
    chain_caught("catches a batch with the chain reading beside it", beside_batch,
                 "a mode that can't happen");

    std::printf("a batch and the connection's state, every pipeline of five - TODO 931\n");
    {
        check(park::changes_connection("USE") && park::changes_connection("HELLO")
              && park::changes_connection("MULTI") && park::changes_connection("EXEC"),
              "USE, HELLO, MULTI and EXEC change the connection");
        check(!park::changes_connection("GET") && !park::changes_connection("KEYS"),
              "GET and KEYS don't");
        check(!park::holds(false, true), "with no batch open, nothing waits");
        check(park::holds(true, true) && !park::holds(true, false),
              "with one open, only a command that changes the connection waits");
        check(park::joins_batch(true, false, false), "once a batch is open, every command joins it");
        check(!park::joins_batch(false, true, true), "under MULTI, KEYS is queued here");
        check(park::joins_batch(false, true, false) && !park::joins_batch(false, false, false),
              "else only an asynchronous command opens one");
    }
    batch_rules bnow;
    batch_clean("as now", bnow);
    size_t pipelines = 0;
    explore_batches(bnow, &pipelines);
    check(pipelines == 6 + 36 + 216 + 1296 + 7776, "every pipeline of one to five commands ran");
    batch_rules old_rules;
    old_rules.no_hold = true;
    old_rules.async_under_multi = true;
    batch_caught("catches the old code", old_rules, "the connection ends in another state");
    batch_rules nohold = bnow;
    nohold.no_hold = true;
    batch_caught("catches a USE that joins the batch", nohold, "the connection ends in another state");
    batch_rules multi_copy = bnow;
    multi_copy.async_under_multi = true;
    batch_caught("catches KEYS under MULTI on a copy", multi_copy, "the connection ends in another state");
    batch_rules use_only = bnow;
    use_only.use_only = true;
    batch_caught("catches a list without MULTI and EXEC", use_only, "the connection ends in another state");

    std::printf("the end of a session, every order of four client moves - TODO 932\n");
    {
        auto e = park::on_read_end(true, false);
        check(!e.net_error && e.shut, "EOF between requests: not an error, and shut");
        e = park::on_read_end(true, true);
        check(e.net_error && e.shut, "EOF mid request: an error, and shut");
        e = park::on_read_end(false, false);
        check(e.net_error && e.shut, "an error: counted, and shut");
        check(park::collects(true, false, -1) && park::collects(false, false, 0)
              && !park::collects(false, false, -1) && !park::collects(false, false, 1),
              "collected on let_go, or a peek of 0");
        check(park::collects(true, true, -1) && !park::collects(false, true, 0),
              "a held session: collected on let_go only - TODO 933");
    }
    end_rules enow;
    end_clean("as now", enow);
    size_t end_states = 0;
    explore_ends(enow, &end_states);
    std::printf("  %zu states\n", end_states);
    end_rules never;
    never.never_shut = true;
    end_caught("catches the old code", never, "a session nothing will run again is kept");
    end_rules eof_open;
    eof_open.open_on_eof = true;
    end_caught("catches a close_notify left open", eof_open, "a session nothing will run again is kept");
    end_rules at_once;
    at_once.shut_at_once = true;
    end_caught("catches a shut before the replies are out", at_once, "a reply is lost");
    end_rules always;
    always.always_collect = true;
    end_caught("catches a collector that drops every session", always,
               "a session its client still uses is dropped");
    end_rules unheld;
    unheld.ignore_held = true;
    end_caught("catches the rule before TODO 933, a peek of 0 drops a held session", unheld,
               "a running session is dropped from the list");

    std::printf("a batch's writes, every batch of four against every reset - TODO 935\n");
    {
        size_t runs = 0;
        const write_rules wnow;
        const auto got = all_writes(wnow, &runs);
        if (!got.empty())
            std::printf("  %s\n", got.c_str());
        char line[120];
        std::snprintf(line, sizeof line, "as now (%zu batches)", runs);
        check(got.empty(), line);
        struct mutant {
            const char* what;
            write_rules r;
        };
        auto ignores = [](bool, bool blocked) {
            return blocked ? park::after_write::park : park::after_write::next_call;
        };
        std::vector<mutant> mutants;
        {
            write_rules m;
            m.reply = ignores;
            mutants.push_back({"as before TODO 935: write_then goes on whatever its write did", m});
        }
        {
            write_rules m;
            m.part = ignores;
            m.reply = ignores;
            mutants.push_back({"no write checked", m});
        }
        {
            write_rules m;
            m.reply = [](bool written, bool blocked) {
                return blocked ? park::after_write::park : park::batch_after_write(written, false);
            };
            mutants.push_back({"a block parks after its write failed", m});
        }
        {
            write_rules m;
            m.reply = [](bool written, bool blocked) {
                return blocked ? park::after_write::stop : park::batch_after_write(written, false);
            };
            mutants.push_back({"a block stops the batch", m});
        }
        {
            write_rules m;
            m.reply = [](bool written, bool blocked) {
                const auto n = park::batch_after_write(written, blocked);
                return n == park::after_write::next_call ? park::after_write::stop : n;
            };
            mutants.push_back({"a batch that stops after its first call", m});
        }
        for (const auto& m : mutants) {
            const auto bad = all_writes(m.r);
            char mline[400];
            std::snprintf(mline, sizeof mline, "catches %s: %s", m.what, bad.empty() ? "missed" : bad.c_str());
            check(!bad.empty(), mline);
        }
    }

    std::printf(failures ? "FAILED\n" : "all passed\n");
    return failures ? 1 : 0;
}
