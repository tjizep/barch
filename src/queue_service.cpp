//
// The queue consumer - TODO 366.
//
// The shape is cron's, because cron already solved the same problem: run stored
// functions on the server's own threads, be stoppable without a detached thread
// nobody can wait for, and survive there being no server yet. A strand
// serialises everything that touches the state, the handler itself is posted to
// the worker context, and `pending` counts handlers that exist so stop() can
// wait for them. See cron.cpp for the long version of that argument.
//
// What is different is what wakes it. A cron job is due at a time, so cron sets
// a timer and sleeps. A message arrives when a sender says so, so a publish
// wakes the consumer directly and the timer is only a backstop for messages a
// crash left behind - which is why `poll` can be minutes and delivery is still
// immediate.
//
#include "queue_service.h"

#include "configuration.h"
#include "cron.h"
#include "function_api.h"
#include "key_space.h"
#include "lzr_log.h"
#include "message_queue.h"
#include "rpc/asio_includes.h"
#include "rpc/server.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <sstream>
#include <cstring>
#include <sys/stat.h>
#include <thread>
#include <unordered_map>
#include <vector>

namespace barch {
namespace mq {

namespace {

#if defined(__cpp_lib_atomic_shared_ptr) && __cpp_lib_atomic_shared_ptr >= 201711L
#define QUEUE_HAS_ATOMIC_SHARED_PTR 1
#else
#define QUEUE_HAS_ATOMIC_SHARED_PTR 0
#endif

#if QUEUE_HAS_ATOMIC_SHARED_PTR
template<typename T> using shared_slot = std::atomic<std::shared_ptr<T>>;
template<typename T> std::shared_ptr<T> load_slot(shared_slot<T>& s) {
    return s.load(std::memory_order_acquire);
}
template<typename T> void store_slot(shared_slot<T>& s, std::shared_ptr<T> v) {
    s.store(std::move(v), std::memory_order_release);
}
#else
template<typename T> using shared_slot = std::shared_ptr<T>;
template<typename T> std::shared_ptr<T> load_slot(shared_slot<T>& s) {
    return std::atomic_load_explicit(&s, std::memory_order_acquire);
}
template<typename T> void store_slot(shared_slot<T>& s, std::shared_ptr<T> v) {
    std::atomic_store_explicit(&s, std::move(v), std::memory_order_release);
}
#endif

/** what one queue is: its declaration, and the file that holds its messages */
struct open_queue {
    barch::foreign::queue_spec spec;
    std::shared_ptr<queue> file;
};

/*
 * The open queues, by the name senders publish to.
 *
 * Separate from the consumer on purpose. A publish has to work whether or not
 * the consumer is armed - barch has no listener during a load, and a function
 * running then can still put a message somewhere durable - and the consumer may
 * be stopped and started while the files stay open.
 */
struct registry {
    std::mutex mut;
    std::unordered_map<std::string, open_queue> queues;
};

registry& reg() {
    static auto* r = new registry();
    return *r;
}

bool make_dir(const std::string& dir, std::string& err) {
    struct stat st{};
    if (::stat(dir.c_str(), &st) == 0) {
        if (S_ISDIR(st.st_mode))
            return true;
        err = dir + " is not a directory";
        return false;
    }
    if (::mkdir(dir.c_str(), 0755) == 0 || errno == EEXIST)
        return true;
    err = "could not create " + dir + ": " + std::strerror(errno);
    return false;
}

/** where this queue's file goes: its own directory, or the server's */
bool dir_for(const barch::foreign::queue_spec& spec, std::string& dir, std::string& err) {
    dir = spec.dir.empty() ? barch::get_queue_dir() : spec.dir;
    if (dir.empty()) {
        /*
         * No directory anywhere. The message is refused rather than held in
         * memory: a queue whose whole purpose is that an accepted message
         * survives the process must not accept one it cannot write.
         */
        err = "queue '" + spec.name + "' has nowhere to write - set queue_dir, or"
              " name a dir in its transport()";
        return false;
    }
    return make_dir(dir, err);
}

/**
 * The queue called `name`, opening its file if this is the first time.
 *
 * The declarations are only consulted on a miss. Reading them means compiling
 * every one of them, which is fine once per queue and not fine per publish.
 */
std::shared_ptr<queue> queue_for(const std::string& name, std::string& err,
                                 barch::foreign::queue_spec* into = nullptr) {
    {
        std::lock_guard lock(reg().mut);
        auto found = reg().queues.find(name);
        if (found != reg().queues.end()) {
            if (into)
                *into = found->second.spec;
            return found->second.file;
        }
    }
    /*
     * One opener at a time, from here down.
     *
     * Without this, a publish and the consumer's tick can both miss the
     * registry and both open the same path: `queue_file` creates under
     * `<path>.tmp` and renames it into place, so the second one finds its own
     * temporary gone and fails with
     *
     *     could not rename the new queue file into place [.../bad.queue.tmp]:
     *     No such file or directory
     *
     * which is what a publish saw. Checking the registry again at the end was
     * not enough - by then both had already built a file. Creating is once per
     * queue, so serialising all of it costs nothing, and the fast path above
     * never reaches here.
     */
    static std::mutex opening;
    std::lock_guard open_one(opening);
    {
        // somebody may have finished opening it while this thread waited
        std::lock_guard lock(reg().mut);
        auto found = reg().queues.find(name);
        if (found != reg().queues.end()) {
            if (into)
                *into = found->second.spec;
            return found->second.file;
        }
    }
    barch::foreign::queue_spec spec;
    bool declared = false;
    for (const auto& e : barch::functions::queue_declarations()) {
        if (!e.parse_err.empty() || !e.spec.is_queue)
            continue;
        if (e.spec.name == name) {
            spec = e.spec;
            declared = true;
            break;
        }
    }
    if (!declared) {
        err = "no queue called '" + name + "' is declared under configuration:queues/";
        return nullptr;
    }
    std::string dir;
    if (!dir_for(spec, dir, err))
        return nullptr;
    barch::aof_sync_setting sync;
    if (!barch::parse_durability(spec.durability, sync)) {
        err = "queue '" + name + "' has a durability this build cannot parse: " + spec.durability;
        return nullptr;
    }
    std::shared_ptr<queue> file;
    try {
        file = std::make_shared<queue>(dir + "/" + name + ".queue", policy_of(sync),
                                       spec.max_attempts);
    } catch (const std::exception& e) {
        err = std::string("could not open queue '") + name + "': " + e.what();
        return nullptr;
    }
    std::lock_guard lock(reg().mut);
    // `opening` above means nobody else can have built one, so this is the
    // ordinary insert rather than a race check
    auto found = reg().queues.find(name);
    if (found != reg().queues.end()) {
        if (into)
            *into = found->second.spec;
        return found->second.file;
    }
    barch::log({"queue", name, "at", dir, "durability", spec.durability,
                "handing messages to", spec.space + ":" + spec.call});
    auto& slot = reg().queues[name];
    slot.spec = spec;
    slot.file = file;
    if (into)
        *into = spec;
    return file;
}

/** what is known about one queue between ticks */
struct queue_state {
    bool running{false};
    uint64_t delivered{0};
    uint64_t failed{0};
    uint64_t dead{0};
    std::string last_error;
};

/** the snapshot status() reads, so it never touches the strand's own state */
struct queue_view {
    bool running{false};
    uint64_t delivered{0};
    uint64_t failed{0};
    uint64_t dead{0};
    uint32_t waiting{0};
    uint32_t in_dead{0};
    std::string last_error;
};

shared_slot<const heap::string_map<queue_view>>& views() {
    static auto* v = new shared_slot<const heap::string_map<queue_view>>();
    return *v;
}

/** how one delivery went */
struct outcome {
    bool handled{false};    // the handler ran and said yes: remove the message
    bool blocked{false};    // nothing to do with the message: leave it, no attempt
    std::string err;
};

/*
 * Hand one message to its function.
 *
 * `blocked` is the distinction that matters here. A handler that throws has
 * failed, and the attempt is counted against the message - enough of those and
 * it is dead lettered. A target space that is not loaded has not failed at all,
 * and counting it would dead letter perfectly good messages during the window
 * where a space has not finished loading. So that case leaves the message and
 * its count exactly as they were, and says so once per tick instead.
 */
/*
 * "default" is how a declaration names the default space, and the default space
 * has no name of its own - the same translation cron's target_space does, and
 * needed for the same reason: without it every declaration that says
 * space = "default" reports that the space is not loaded, forever.
 */
std::string target_space(const std::string& declared) {
    return declared == "default" ? std::string{} : declared;
}

outcome deliver(const barch::foreign::queue_spec& spec, const message& m) {
    outcome out;
    try {
        auto target = target_space(spec.space);
        if (!barch::is_keyspace(target)) {
            out.blocked = true;
            out.err = "target space '" + spec.space + "' is not loaded";
            return out;
        }
        auto space = barch::get_keyspace(target);
        if (!space) {
            out.blocked = true;
            out.err = "target space '" + spec.space + "' is not loaded";
            return out;
        }
        heap::vector<std::string> args;
        args.push_back(m.data);
        args.push_back(std::to_string(m.sequence));
        args.push_back(std::to_string(m.attempts));
        Variable result;
        std::string err;
        if (!barch::functions::call_as(space, spec.user, spec.call, args, result, err)) {
            out.err = err.empty() ? "the handler failed" : err;
            return out;
        }
        out.handled = true;
    } catch (const std::exception& e) {
        // this runs on a worker thread, so an exception that escaped would be an
        // uncaught one and take the process down - the one thing a message must
        // not be able to do to an otherwise fine server
        out.err = e.what();
    } catch (...) {
        out.err = "the handler threw something that was not a std::exception";
    }
    return out;
}

struct consumer : std::enable_shared_from_this<consumer> {
    explicit consumer(asio::io_context& io)
        : io(io), strand(asio::make_strand(io)), timer(strand) {}

    asio::io_context& io;
    asio::strand<asio::io_context::executor_type> strand;
    asio::steady_timer timer;
    std::atomic<bool> stopping{false};
    std::atomic<int64_t> pending{0};
    /** strand only, from here down */
    heap::string_map<queue_state> states;

    std::shared_ptr<void> token() {
        pending.fetch_add(1, std::memory_order_acq_rel);
        auto self = shared_from_this();
        return std::shared_ptr<void>(this, [self](void*) {
            self->pending.fetch_sub(1, std::memory_order_acq_rel);
        });
    }

    void begin() {
        auto self = shared_from_this();
        auto t = token();
        asio::post(strand, [this, self, t]() { tick(); });
    }

    void wake() {
        if (stopping.load())
            return;
        auto self = shared_from_this();
        auto t = token();
        asio::post(strand, [this, self, t]() { tick(); });
    }

    /** strand only */
    void publish_views() {
        auto snap = std::make_shared<heap::string_map<queue_view>>();
        for (const auto& kv : states) {
            queue_view v;
            v.running = kv.second.running;
            v.delivered = kv.second.delivered;
            v.failed = kv.second.failed;
            v.dead = kv.second.dead;
            v.last_error = kv.second.last_error;
            std::string err;
            if (auto f = queue_for(kv.first, err)) {
                v.waiting = f->size();
                v.in_dead = f->dead_size();
            }
            (*snap)[kv.first] = v;
        }
        store_slot<const heap::string_map<queue_view>>(views(), snap);
    }

    /** strand only */
    void arm(uint64_t wait_ms) {
        if (stopping.load())
            return;
        auto self = shared_from_this();
        auto t = token();
        timer.expires_after(std::chrono::milliseconds(wait_ms));
        timer.async_wait([this, self, t](const std::error_code& ec) {
            if (ec || stopping.load())
                return;
            tick();
        });
    }

    /** strand only */
    void fire(const std::string& name, const barch::foreign::queue_spec& spec,
              const std::shared_ptr<queue>& file, const message& m) {
        auto self = shared_from_this();
        auto t = token();
        asio::post(io, [this, self, t, name, spec, file, m]() {
            if (stopping.load()) {
                // it still has to report, or the queue stays "running" forever
                report(name, outcome{false, true, "the consumer stopped before the handler ran"},
                       file, m);
                return;
            }
            report(name, deliver(spec, m), file, m);
        });
    }

    /**
     * any thread: one finished delivery, back to the strand.
     *
     * The message is removed *here* rather than before the call, which is the
     * whole reason peek and remove are separate: a crash between the handler
     * finishing and this line means the message is still in the file and is
     * handled again on the next start. At-least-once, and never none.
     */
    void report(const std::string& name, outcome out,
                const std::shared_ptr<queue>& file, const message& m) {
        auto self = shared_from_this();
        auto t = token();
        asio::post(strand, [this, self, t, name, out, file, m]() {
            auto& s = states[name];
            s.running = false;
            if (out.handled) {
                file->remove();
                ++s.delivered;
                s.last_error.clear();
            } else if (out.blocked) {
                // not the message's fault: no attempt counted, nothing removed
                s.last_error = out.err;
            } else {
                ++s.failed;
                if (file->failed(m))
                    ++s.dead;
                s.last_error = out.err;
            }
            publish_views();
            // straight back round while there is more, rather than waiting for a
            // poll: a backlog left by a restart should drain at once
            if (!file->empty() && !out.blocked)
                tick();
            else
                arm(idle_wait());
        });
    }

    /** strand only. the shortest poll any declared queue asked for */
    uint64_t idle_wait() const {
        return poll_ms;
    }

    /** strand only */
    void tick() {
        if (stopping.load())
            return;
        auto declared = barch::functions::queue_declarations();
        uint64_t shortest = 300000;     // five minutes if nothing says otherwise
        for (auto& e : declared) {
            if (!e.spec.is_queue && e.parse_err.empty())
                continue;
            auto& s = states[e.spec.name.empty() ? e.name : e.spec.name];
            if (!e.parse_err.empty()) {
                s.last_error = e.parse_err;
                continue;
            }
            uint64_t ms = 30000;
            std::string perr;
            if (barch::cron::parse_duration(e.spec.poll, ms, perr))
                shortest = std::min(shortest, ms);
            if (!e.spec.enabled || s.running)
                continue;
            std::string err;
            barch::foreign::queue_spec spec;
            auto file = queue_for(e.spec.name, err, &spec);
            if (!file) {
                s.last_error = err;
                continue;
            }
            message m;
            if (!file->peek(m))
                continue;               // nothing waiting
            s.running = true;
            fire(e.spec.name, spec, file, m);
        }
        poll_ms = shortest;
        publish_views();
        arm(shortest);
    }

    uint64_t poll_ms{30000};
};

shared_slot<consumer>& current() {
    static auto* s = new shared_slot<consumer>();
    return *s;
}

} // namespace

void request_rescan() {
    if (auto c = load_slot(current()))
        c->wake();
}

bool publish(const std::string& name, const std::string& data,
             uint64_t& sequence, std::string& err) {
    auto file = queue_for(name, err);
    if (!file)
        return false;
    try {
        sequence = file->publish(data);
    } catch (const std::exception& e) {
        err = std::string("could not write to queue '") + name + "': " + e.what();
        return false;
    }
    /*
     * Wake the consumer now the message is on disk, not before. Waking first
     * would let the consumer look, find nothing, and go back to sleep for a
     * whole poll interval with a message sitting there.
     */
    if (auto c = load_slot(current()))
        c->wake();
    return true;
}

std::string status() {
    std::ostringstream o;
    auto declared = barch::functions::queue_declarations();
    auto snap = load_slot(views());
    bool first = true;
    for (const auto& e : declared) {
        if (!first) o << "\n";
        first = false;
        queue_view v;
        if (snap) {
            auto found = snap->find(e.spec.name.empty() ? e.name : e.spec.name);
            if (found != snap->end())
                v = found->second;
        }
        o << "declared=" << e.name
          << " name=" << e.spec.name
          << " space=" << e.spec.space
          << " call=" << e.spec.call
          << " user=" << e.spec.user
          << " enabled=" << (e.spec.enabled ? "on" : "off")
          << " durability=" << e.spec.durability
          << " max_attempts=" << e.spec.max_attempts
          << " waiting=" << v.waiting
          << " delivered=" << v.delivered
          << " failed=" << v.failed
          << " dead=" << v.in_dead
          << " running=" << (v.running ? "yes" : "no");
        if (!e.parse_err.empty())
            o << " error=" << e.parse_err;
        else if (!v.last_error.empty())
            o << " last_error=" << v.last_error;
    }
    if (first)
        return "no queues are configured";
    return o.str();
}

void start() {
    if (load_slot(current()))
        return;                 // already armed, the same as cron::start
    auto* io = barch::server::worker_io();
    if (!io) {
        barch::log({"queues waiting for a server to consume on"});
        return;
    }
    auto c = std::make_shared<consumer>(*io);
    store_slot<consumer>(current(), c);
    c->begin();
}

void stop() {
    auto c = load_slot(current());
    if (!c)
        return;
    store_slot<consumer>(current(), nullptr);
    c->stopping.store(true);
    {
        // posted, because the timer belongs to the strand and only the strand
        // may touch it
        auto t = c->token();
        asio::post(c->strand, [c, t]() { c->timer.cancel(); });
    }
    /*
     * Wait for every handler to be gone before letting the consumer go, for the
     * reason cron::stop gives at length: the last reference dropping destroys
     * the timer, and the timer belongs to an io_context the caller is usually
     * about to destroy. Capped, and leaked rather than waited on forever, since
     * a leak at shutdown costs nothing and touching a dead io_context crashes.
     */
    for (int i = 0; i < 10000 && c->pending.load() > 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    if (c->pending.load() > 0) {
        barch::err({"queue handlers still outstanding at stop", c->pending.load()});
        new std::shared_ptr<consumer>(c); // deliberately never freed
    }
    /*
     * The files stay open. A publish is allowed with no consumer armed - that is
     * what makes a message written during a load durable - and closing them here
     * would turn a stop into a reason for a publish to fail.
     */
}

}
}
