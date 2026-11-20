#ifndef BARCH_STREAM_CORE_H
#define BARCH_STREAM_CORE_H
/*
 * The rules of a streamed reply - TODO 943.
 *
 * A worker queues a reply's bytes with stream_out and waits in stream_wait while too
 * much is queued. The socket's thread writes them with stream_next, one
 * async_write_some at a time, and each completion lets the worker on. A write that
 * fails, a wait that times out and a server stop each fail the stream.
 *
 * The decisions are here, from plain values. asio_resp_session.h holds the stream's
 * lock, does the writes and the posts, and shuts the socket.
 *
 * This header includes no locks and no clock, and TestCoreBoundary fails if it starts
 * to. test/streamtest.cpp runs every order of a worker's queues and flushes against
 * every order of the socket's writes, each taking one byte, all of it or failing, with
 * a timeout and a server stop at every point.
 */
#include <cstdint>

namespace barch::stream_core {
    /** what stream_wait does with the backlog it sees */
    enum class on_wait {
        /** under the limit: queue, or for a flush, all of it is on the socket */
        go,
        /** at or over it: wait for a write to complete */
        wait,
        /** the stream failed: the client was let go, and the batch stops */
        give_up,
    };

    inline on_wait wait_step(bool failed, uint64_t backlog, uint64_t below) {
        if (failed)
            return on_wait::give_up;
        return backlog < below ? on_wait::go : on_wait::wait;
    }

    /** a worker queued bytes: post stream_next unless a write is out or posted */
    inline bool posts_next(bool busy) {
        return !busy;
    }

    /** what stream_next does on the socket's thread */
    enum class on_next {
        /** the rest of the buffer being written */
        write,
        /** what the worker queued since becomes the buffer, and is written */
        swap,
        /** nothing left, or the stream failed: clear it all, and not busy */
        stop,
    };

    /*
     * Before TODO 943 a stream that failed by a timeout, with a write half done, still
     * wrote the rest. Now nothing starts once the stream failed.
     */
    inline on_next next(bool failed, bool written_all, bool pending_empty) {
        if (failed)
            return on_next::stop;
        if (!written_all)
            return on_next::write;
        return pending_empty ? on_next::stop : on_next::swap;
    }

    /** what a write's completion does, under the stream's lock */
    struct after_write {
        uint64_t backlog{};
        /** the client took bytes: a waiting worker sees progress */
        bool progress{};
        /** one net error for a stream, however many writes fail */
        bool count_error{};
        bool failed{};
        /** shut the socket and set let_go. Its read chain is down for the batch */
        bool let_go{};
    };

    /*
     * Before TODO 943 a failed write only failed the stream. The worker gave up and the
     * read chain stayed down, so nothing read the error. A reset socket with input still
     * unread peeks as that input, not 0, and the collector kept the session for good.
     */
    inline after_write written(bool error, bool failed, uint64_t backlog, uint64_t length) {
        after_write w;
        if (error) {
            w.backlog = backlog;
            w.count_error = !failed;
            w.failed = true;
            w.let_go = true;
            return w;
        }
        w.backlog = backlog - (length < backlog ? length : backlog);
        w.progress = true;
        w.failed = failed;
        return w;
    }
}

#endif
