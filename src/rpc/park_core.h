#ifndef BARCH_PARK_CORE_H
#define BARCH_PARK_CORE_H
/*
 * The decisions a parked connection makes, with nothing else in them - TODO 920.
 *
 * resp_session parks a client on a blocking command and then reacts to three
 * things: its timer, a wake from another thread, and what its socket brings.
 * What it does in each case is decided here, from plain values. The session does
 * the I/O and the locking and hands the results in.
 *
 * This header includes no asio, no locks, no clocks and no socket calls, and
 * TestCoreBoundary fails if it starts to. That keeps it small enough to test
 * every short sequence of events against it - test/parkcoretest.cpp.
 */
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>

namespace barch::park {

    /** A park's timer. Not armed means it waits for ever. */
    struct timer_plan {
        bool armed{false};
        int64_t wait_ms{0};
        // when the park's time runs out, kept through a park again. 0 when not armed
        int64_t deadline{0};
    };

    /**
     * The timer for a park. `again` is a waiter woken with nothing for it, parking
     * once more: it keeps the time it had left - TODO 879. A deadline already gone
     * stays gone, so a wake that lands before the timer fires is still a timeout.
     */
    inline timer_plan plan_timer(uint64_t block_to_ms, bool again, int64_t deadline, int64_t now) {
        if (block_to_ms == 0 || block_to_ms >= (uint64_t) std::numeric_limits<int64_t>::max())
            return {};
        if (again && deadline) {
            const int64_t left = deadline - now;
            return {true, left > 0 ? left : 0, deadline};
        }
        const auto wait = (int64_t) block_to_ms;
        return {true, wait, now + wait};
    }

    /** Whether a handler made for park `gen` still has a park to act on - TODO 878. */
    inline bool still_parked(uint64_t gen, uint64_t current, bool has_blocks) {
        return gen == current && has_blocks;
    }

    /** The timer fired. True if it should answer with a timeout. */
    inline bool timer_answers(bool cancelled, bool parked) {
        return !cancelled && parked;
    }

    enum class wake {
        pass_on,    // the park is gone: the next waiter on the key gets the turn
        time_out,   // the value came after the deadline: still a timeout, and pass on
        run         // run the waiter
    };

    /** A wake reached the session's own thread. */
    inline wake on_wake(bool parked, int64_t waiter_deadline, int64_t now) {
        if (!parked)
            return wake::pass_on;
        if (waiter_deadline && now >= waiter_deadline)
            return wake::time_out;
        return wake::run;
    }

    /*
     * A parked session keeps one socket read out - TODO 923. What it brings is kept
     * in the parser and runs once the block answers, so a pipelined command waits
     * its turn, and a hang-up shows as the read ending. A wait that left the bytes
     * in the socket fired again at once on every arm, because asio arms with
     * EPOLL_CTL_MOD and the kernel reports what is already there.
     */
    enum class arrived {
        run,    // the read chain took this read over: run what came
        keep,   // keep it for when the block answers, and read again
        hold,   // keep it and read no more: the block answered, and the chain reads next
        drop    // past the limit: close the client
    };

    /**
     * A read came back with bytes. `for_park` says the park started it and the chain
     * has not taken it over. `most` is rpc_query_buffer_limit.
     *
     * A park never stops reading while the client is within the limit - TODO 924.
     * A server that stops reading closes the TCP window, and the client's FIN then
     * waits behind its unsent data on the client's side. Nothing can see that client
     * go. So past the limit the client is closed instead.
     */
    inline arrived on_arrival(bool for_park, bool parked, size_t buffered, size_t most) {
        if (!for_park)
            return arrived::run;
        if (!parked)
            return arrived::hold;
        return buffered > most ? arrived::drop : arrived::keep;
    }

    /** Whether a park starts a read: parked, and none out. */
    inline bool watch_now(bool parked, bool read_out) {
        return parked && !read_out;
    }

    /**
     * The chain wants to read. False when a read is already out: the chain takes it
     * over instead of starting a second one on the same socket.
     */
    inline bool chain_reads(bool read_out) {
        return !read_out;
    }

    /*
     * The read chain's own choices, and the modes it moves between - TODO 930.
     *
     * A session is reading, paused for its replies to drain, parked on a blocking
     * command, or running an asynchronous batch, which can park part way. Six
     * values say which, and only some of their combinations can happen.
     * `mode_of` names the mode, or says the combination is one that can't.
     */
    enum class next {
        pause,      // replies past rpc_output_high_water: read nothing until they drain
        run_kept,   // requests kept in the parser run first, with nothing new read
        read        // read the socket
    };

    /** continue_reading. `backlog` is reply bytes handed to send() and not yet written */
    inline next continue_reading(size_t backlog, size_t high, bool parse_pending) {
        if (backlog >= high)
            return next::pause;
        return parse_pending ? next::run_kept : next::read;
    }

    /** resume_if_drained: a paused chain goes on below half the high water mark */
    inline bool resumes(bool paused, size_t backlog, size_t high) {
        return paused && backlog < high / 2;
    }

    enum class after_answer {
        rest_of_batch,  // a batch stopped on the block: run the rest of it
        run_kept        // what was pipelined behind the block, then read
    };

    /** resume_after_blocks, once the block's reply is on the socket */
    inline after_answer after_block(bool batch_waiting) {
        return batch_waiting ? after_answer::rest_of_batch : after_answer::run_kept;
    }

    struct chain {
        bool paused{false};         // read_paused
        bool read_out{false};
        bool for_park{false};       // read_for_park
        bool parked{false};         // caller.has_blocks()
        bool in_batch{false};       // from a batch's start to its end, parks included
        bool batch_waiting{false};  // pending_batch: a batch stopped on a block
    };

    enum class mode {
        reading,        // the chain reads, runs, or has a read out
        paused,         // waiting for replies to drain
        parked,         // waiting on a block, one read out to see a hang-up
        batch,          // a batch runs on the workers
        batch_parked,   // a batch stopped on a block
        impossible      // a combination no path makes
    };

    /*
     * Seen between handlers, on the socket's thread. Each rule is one the shell keeps
     * without saying so:
     *   - a park's read is a read out;
     *   - a paused chain has no park and no batch: continue_reading, which pauses,
     *     is only reached once both are done;
     *   - a park keeps one read out, or a hang-up goes unseen (TODO 226);
     *   - the chain's own read, one the park did not start, is out only while
     *     reading. The chain reads from continue_reading alone, and what that read
     *     brings runs, so it must not be out while anything waits;
     *   - a park inside a batch keeps the batch to go on with. The batch is kept a
     *     little longer than the park: while the answer is written, and after a
     *     hang-up, when nothing runs it again.
     */
    inline mode mode_of(const chain& c) {
        if (c.for_park && !c.read_out)
            return mode::impossible;
        if (c.batch_waiting && !c.in_batch)
            return mode::impossible;
        if (c.parked && c.in_batch && !c.batch_waiting)
            return mode::impossible;
        if (c.read_out && !c.for_park && (c.paused || c.parked || c.in_batch))
            return mode::impossible;
        if (c.paused)
            return c.parked || c.in_batch ? mode::impossible : mode::paused;
        if (c.parked)
            return !c.read_out ? mode::impossible : c.in_batch ? mode::batch_parked : mode::parked;
        if (c.in_batch)
            return mode::batch;
        return mode::reading;
    }

    /*
     * A batch - TODO 931. Each of its calls runs on a copy of the session's caller,
     * made when the batch is read. A call that changes the connection changes only
     * its copy: the calls after it were copied before it ran, and the session never
     * sees it. USE, HELLO and MULTI in a batch were lost that way. So a batch stops
     * ahead of such a command. It is held, and runs on the session's caller once the
     * batch is done, before anything after it is read.
     */
    inline bool changes_connection(std::string_view upper) {
        for (const char* n : {"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH", "USE", "SELECT", "AUTH",
                              "HELLO", "CLIENT", "RESET", "READONLY", "READWRITE", "BEGINTX", "ABORTTX",
                              "COMMITTX"})
            if (upper == n)
                return true;
        return false;
    }

    /** a command read while a batch is open, and one that changes the connection, waits */
    inline bool holds(bool batch_open, bool changes) {
        return batch_open && changes;
    }

    /**
     * Whether a command joins a batch. Once one is open, all do, or replies leave out
     * of order. Under MULTI none opens one: a command there is only queued, on the
     * session's caller, where EXEC finds it. On a copy it was queued and lost.
     */
    inline bool joins_batch(bool batch_open, bool asynch, bool buffering) {
        if (batch_open)
            return true;
        return asynch && !buffering;
    }

    /*
     * A batch call's reply - TODO 935. A worker writes each call's reply in up to
     * four parts: what was carried in from before the batch, what the call streamed,
     * the rest of the stream, and the reply itself (write_then). Any part that fails
     * means the client is gone. The batch stops there and the session is let go: the
     * calls behind it have nowhere to send a reply, and a write among them would run
     * for a client that can't see it. write_then went on to the next call whatever
     * its write did.
     */
    enum class after_write {
        next_call,      // run the batch's next call
        park,           // the call registered a block: park on the socket's thread
        stop            // a write failed: the client is let go, nothing more runs
    };

    inline after_write batch_after_write(bool written, bool blocked) {
        if (!written)
            return after_write::stop;
        return blocked ? after_write::park : after_write::next_call;
    }

    /*
     * The end of a session - TODO 932. The collector frees a session on let_go, or
     * when a peek of its socket reads 0: the peer sent its FIN and every byte before
     * it was read. A read that ends the chain did neither for a TLS session. A bad
     * record or a close_notify is read through the ssl stream, the TCP stays open,
     * and the peek reads nothing. The session stayed as long as its client did.
     */
    struct read_end {
        bool net_error;     // counted in net_errors
        bool shut;          // shut the socket once what is queued is out, and let go
    };

    /** A read came back with an error, or the end. EOF between requests is the normal close */
    inline read_end on_read_end(bool eof, bool mid_request) {
        return {!eof || mid_request, true};
    }

    /*
     * The collector's rule: what `let_go`, `held` and a peek of the socket say. `held`
     * is true while anything but the session list holds the session: a read, a write,
     * or a worker's call. A client can send its FIN while a worker still waits for it
     * to take a streamed reply. Its peek reads 0, but the session is still running.
     * Dropped from the list, stop() could not reach it, and the worker waited for ever
     * at rpc_client_max_wait_ms 0 - TODO 933. A held session reaches let_go itself.
     * The peek is for one that nothing holds.
     */
    inline bool collects(bool let_go, bool held, long peek) {
        return let_go || (!held && peek == 0);
    }
}
#endif //BARCH_PARK_CORE_H
