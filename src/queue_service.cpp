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
#include "data_dir.h"

#include <atomic>
#include <chrono>
#include <functional>
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
    bool on_timer{false};           // durability = timer: synced by sync_timer_queues
    /*
     * A delivery of this queue's head is out - TODO 517. Here rather than in
     * the consumer because it has to outlive one: server::start() replaces the
     * consumer, and a handler parked on I/O is still running when it does. The
     * new consumer used to start with nothing marked running and hand the same
     * head out again while the first copy was still working on it.
     */
    bool out{false};
    /*
     * Its declaration is still there - TODO 516. A removed one is marked rather
     * than erased: a delivery still out keeps its claim, and a queue declared
     * again under the same name comes back to this entry and this file instead
     * of a second queue object on the same path.
     */
    bool declared{true};
    std::string apply_err;          // the last reason a redeclaration couldn't be applied
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

/** take the queue for one delivery; false while one is already out - TODO 517 */
bool claim(const std::string& name) {
    std::lock_guard lock(reg().mut);
    auto found = reg().queues.find(name);
    if (found == reg().queues.end() || found->second.out)
        return false;
    found->second.out = true;
    return true;
}

void release(const std::string& name) {
    std::lock_guard lock(reg().mut);
    auto found = reg().queues.find(name);
    if (found != reg().queues.end())
        found->second.out = false;
}

bool is_out(const std::string& name) {
    std::lock_guard lock(reg().mut);
    auto found = reg().queues.find(name);
    return found != reg().queues.end() && found->second.out;
}

/** wake whichever consumer is armed now, if any. Defined below the consumer */
void wake_current();

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
    // a relative one is in the data directory, where it stays - TODO 526
    dir = spec.dir.empty() ? barch::get_queue_dir() : barch::data_path(spec.dir);
    if (dir.empty()) {
        /*
         * No directory anywhere. The message is refused rather than held in
         * memory: a queue whose whole purpose is that an accepted message
         * survives the process must not accept one it cannot write.
         */
        err = "queue '" + spec.name + "' has nowhere to write - set queue_dir, or"
              " name a dir in its service()";
        return false;
    }
    return make_dir(dir, err);
}

/*
 * One opener at a time.
 *
 * Without this, a publish and the consumer's tick can both miss the registry
 * and both open the same path: `queue_file` creates under `<path>.tmp` and
 * renames it into place, so the second one finds its own temporary gone and
 * fails with
 *
 *     could not rename the new queue file into place [.../bad.queue.tmp]:
 *     No such file or directory
 *
 * which is what a publish saw. It also serialises applying a redeclaration
 * (TODO 516), which can open a file too. Both are rare, so serialising all of
 * it costs nothing, and the fast path in queue_for never takes it.
 */
std::mutex& opening() {
    static auto* m = new std::mutex();
    return *m;
}

bool same_declaration(const barch::foreign::queue_spec& a, const barch::foreign::queue_spec& b) {
    return a.name == b.name && a.space == b.space && a.call == b.call && a.user == b.user
        && a.durability == b.durability && a.dir == b.dir && a.poll == b.poll
        && a.max_attempts == b.max_attempts && a.enabled == b.enabled;
}

/**
 * `opening()` held. Make the entry for `name` match `spec` - TODO 516: open
 * the file if there's no entry, move to a new file if the directory changed,
 * and otherwise change the open queue in place. The consumer and a publish
 * read the spec from the entry, so a new call, user or space applies from the
 * next delivery.
 */
std::shared_ptr<queue> apply_locked(const std::string& name, const barch::foreign::queue_spec& spec,
                                    std::string& err) {
    std::string dir;
    if (!dir_for(spec, dir, err))
        return nullptr;
    barch::aof_sync_setting sync;
    if (!barch::parse_durability(spec.durability, sync)) {
        err = "queue '" + name + "' has a durability this build cannot parse: " + spec.durability;
        return nullptr;
    }
    const std::string path = dir + "/" + name + ".queue";
    std::shared_ptr<queue> current;
    barch::foreign::queue_spec was;
    bool was_declared = true;
    {
        std::lock_guard lock(reg().mut);
        auto found = reg().queues.find(name);
        if (found != reg().queues.end()) {
            current = found->second.file;
            was = found->second.spec;
            was_declared = found->second.declared;
        }
    }
    if (current && current->where() == path) {
        if (was_declared && same_declaration(was, spec))
            return current;
        try {
            current->set_policy(policy_of(sync));
            current->set_max_attempts(spec.max_attempts);
        } catch (const std::exception& e) {
            err = std::string("could not apply the new declaration of queue '") + name + "': " + e.what();
            return nullptr;
        }
        std::lock_guard lock(reg().mut);
        auto& slot = reg().queues[name];
        slot.spec = spec;
        slot.declared = true;
        slot.on_timer = sync.mode == barch::aof_sync_setting::timer;
        barch::log({"queue", name, was_declared ? "follows its new declaration:" : "is declared again:",
                    "handing messages to", spec.space + ":" + spec.call, "as", spec.user,
                    "durability", spec.durability});
        return current;
    }
    // new, or moved to another directory
    std::shared_ptr<queue> file;
    try {
        file = std::make_shared<queue>(path, policy_of(sync), spec.max_attempts);
    } catch (const std::exception& e) {
        err = std::string("could not open queue '") + name + "': " + e.what();
        return nullptr;
    }
    std::lock_guard lock(reg().mut);
    auto& slot = reg().queues[name];
    if (current) {
        // what's in the old file stays there: nothing moves messages between
        // directories, and it's said out loud rather than done quietly
        barch::log({"queue", name, "moved from", current->where(), "to", path, "-",
                    (uint64_t) current->size(), "messages stay in the old file"});
    } else {
        barch::log({"queue", name, "at", dir, "durability", spec.durability,
                    "handing messages to", spec.space + ":" + spec.call});
    }
    // a delivery from the old file still out keeps `out` set, so nothing from
    // the new one goes out until it settles
    slot.spec = spec;
    slot.file = file;
    slot.declared = true;
    slot.on_timer = sync.mode == barch::aof_sync_setting::timer;
    return file;
}

/**
 * The queue called `name`, opening its file if this is the first time.
 *
 * The declarations are only consulted on a miss. Reading them means compiling
 * every one of them, which is fine once per queue and not fine per publish.
 * A queue whose declaration was removed is a miss too, and refused - TODO 516.
 */
std::shared_ptr<queue> queue_for(const std::string& name, std::string& err,
                                 barch::foreign::queue_spec* into = nullptr) {
    const auto cached = [&]() -> std::shared_ptr<queue> {
        std::lock_guard lock(reg().mut);
        auto found = reg().queues.find(name);
        if (found == reg().queues.end() || !found->second.declared)
            return nullptr;
        if (into)
            *into = found->second.spec;
        return found->second.file;
    };
    if (auto f = cached())
        return f;
    std::lock_guard open_one(opening());
    // somebody may have finished opening it while this thread waited
    if (auto f = cached())
        return f;
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
    auto file = apply_locked(name, spec, err);
    if (file && into)
        *into = spec;
    return file;
}

/**
 * Bring every open queue in line with the declarations - TODO 516. Called on a
 * rescan and at the top of every tick, so a declaration that changes some other
 * way (a LOAD, a replica) is followed within a poll too.
 *
 * The declarations are read here, under opening(), not handed in. The tick used
 * to read them and then wait for the lock, and a SETF's own rescan could apply
 * a newer declaration in that gap, which the tick then undid with its older copy
 * until the next rescan. A durability changed from timer to each went back to
 * timer for the push straight after - TODO 604. Read under the lock, whoever
 * applies last has read last.
 */
void reconcile() {
    std::lock_guard open_one(opening());
    std::unordered_map<std::string, barch::foreign::queue_spec> want;
    for (const auto& e : barch::functions::queue_declarations()) {
        if (e.parse_err.empty() && e.spec.is_queue)
            want.emplace(e.spec.name, e.spec);      // the first declaration of a name wins
    }
    std::vector<std::string> names;
    {
        std::lock_guard lock(reg().mut);
        for (const auto& kv : reg().queues)
            names.push_back(kv.first);
    }
    for (const auto& name : names) {
        auto w = want.find(name);
        if (w == want.end()) {
            std::lock_guard lock(reg().mut);
            auto& slot = reg().queues[name];
            if (slot.declared) {
                slot.declared = false;
                barch::log({"queue", name, "is no longer declared: pushes to it are refused, and",
                            (uint64_t) slot.file->size(), "messages stay in", slot.file->where()});
            }
            continue;
        }
        std::string err;
        const bool ok = apply_locked(name, w->second, err) != nullptr;
        std::lock_guard lock(reg().mut);
        auto& slot = reg().queues[name];
        if (!ok && err != slot.apply_err)
            barch::err({err});                      // once, not once a tick
        slot.apply_err = ok ? std::string() : err;
    }
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
 * One message handed out, from the claim to its outcome - TODO 517.
 *
 * Settling it is the queue's business, not the consumer's: remove() on a
 * success, failed() on a failure, then give the claim back. The queue is
 * thread safe and stays open across a server restart, so this can run on
 * whatever thread the handler finished on, whether or not the consumer that
 * fired it is still there. It used to go through that consumer's strand, and
 * after a restart it was thrown away: a success was never removed and a
 * failure never counted.
 *
 * Settled exactly once. If it's dropped without being settled - the handler
 * never ran because its io_context went with a restart - the claim is given
 * back and the message stays for the next delivery, with no attempt counted.
 */
struct delivery {
    std::string name;
    std::shared_ptr<queue> file;
    message m;
    std::atomic<bool> settled{false};

    delivery(std::string name, std::shared_ptr<queue> file, message m)
        : name(std::move(name)), file(std::move(file)), m(std::move(m)) {}
    delivery(const delivery&) = delete;
    delivery& operator=(const delivery&) = delete;

    ~delivery() {
        if (!settled.exchange(true)) {
            release(name);
            wake_current();
        }
    }

    /** false when it was already settled. `dead` says it was dead lettered */
    bool settle(const outcome& out, bool& dead) {
        dead = false;
        if (settled.exchange(true))
            return false;
        try {
            if (out.handled)
                file->remove();
            else if (!out.blocked)
                dead = file->failed(m);
        } catch (const std::exception& e) {
            barch::err({"queue", name, "could not settle message", m.sequence, e.what()});
        }
        release(name);          // after the remove, so nobody peeks the old head
        return true;
    }
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

void deliver(const barch::foreign::queue_spec& spec, const message& m,
             std::function<void(outcome)> answer) {
    // exactly one answer, even if something throws after the handler has already
    // answered through the completion
    auto once = std::make_shared<std::atomic<bool>>(false);
    auto reply = [once, answer = std::move(answer)](outcome out) {
        if (!once->exchange(true))
            answer(std::move(out));
    };
    try {
        auto target = target_space(spec.space);
        auto space = barch::keyspace_exists(target) ? barch::get_keyspace(target) : nullptr;
        if (!space) {
            // neither open nor saved: a saved one is opened above - TODO 439
            reply(outcome{false, true, "target space '" + spec.space + "' does not exist"});
            return;
        }
        heap::vector<std::string> args;
        args.push_back(m.data);
        args.push_back(std::to_string(m.sequence));
        args.push_back(std::to_string(m.attempts));
        // asynchronous: a handler parked on I/O holds no thread of the consumer's,
        // and its outcome arrives from the function pool when it ends - TODO 436
        barch::functions::call_as_async(space, spec.user, spec.call, args,
            [reply](bool ok, Variable, std::string err) {
                if (ok)
                    reply(outcome{true, false, {}});
                else
                    reply(outcome{false, false, err.empty() ? "the handler failed" : err});
            });
    } catch (const std::exception& e) {
        // this runs on a worker thread, so an exception that escaped would be an
        // uncaught one and take the process down - the one thing a message must
        // not be able to do to an otherwise fine server
        reply(outcome{false, false, e.what()});
    } catch (...) {
        reply(outcome{false, false, "the handler threw something that was not a std::exception"});
    }
}

struct consumer : std::enable_shared_from_this<consumer> {
    explicit consumer(asio::io_context& io)
        : io(io), strand(asio::make_strand(io)), timer(strand) {}

    asio::io_context& io;
    asio::strand<asio::io_context::executor_type> strand;
    asio::steady_timer timer;
    std::atomic<bool> stopping{false};
    /** stop() stopped waiting for handlers, so a late one must not post */
    std::atomic<bool> abandoned{false};
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
            v.running = is_out(kv.first);   // the registry's, which outlives a consumer
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
        auto d = std::make_shared<delivery>(name, file, m);
        asio::post(io, [this, self, t, spec, d]() {
            // stopping: `d` goes unsettled, which gives the claim back and
            // leaves the message for the next consumer
            if (stopping.load())
                return;
            // the token rides with the completion, so stop() waits for a handler
            // that is still out on I/O the same as one that is running
            deliver(spec, d->m, [this, self, t, d](outcome out) {
                bool dead = false;
                if (!d->settle(out, dead))
                    return;
                if (abandoned.load()) {
                    // stop() gave up waiting and its context may be gone: the
                    // message is settled, only this consumer's counters are lost
                    wake_current();
                    return;
                }
                record(d->name, std::move(out), dead, d->file);
            });
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
    void record(const std::string& name, outcome out, bool dead,
                const std::shared_ptr<queue>& file) {
        auto self = shared_from_this();
        auto t = token();
        asio::post(strand, [this, self, t, name, out, dead, file]() {
            // the message itself was settled by the delivery - this is counting
            auto& s = states[name];
            s.running = false;
            if (out.handled) {
                ++s.delivered;
                s.last_error.clear();
            } else if (out.blocked) {
                // not the message's fault: no attempt counted, nothing removed
                s.last_error = out.err;
            } else {
                ++s.failed;
                if (dead)
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
        // an open queue follows its declaration, or stops taking pushes - TODO 516
        reconcile();
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
            // a delivery from before a server restart may still be out - TODO 517
            if (!claim(e.spec.name))
                continue;
            message m;
            try {
                if (!file->peek(m)) {
                    release(e.spec.name);
                    continue;           // nothing waiting
                }
            } catch (const std::exception& ex) {
                release(e.spec.name);
                s.last_error = std::string("could not read the queue: ") + ex.what();
                continue;
            }
            /*
             * On disk before the handler runs, so a handler that takes the
             * process down still counts as an attempt - TODO 511. If it can't
             * be written, the handler doesn't run: an uncounted delivery is how
             * a poison message crash looped.
             */
            try {
                file->started(m);
            } catch (const std::exception& ex) {
                release(e.spec.name);
                s.last_error = std::string("could not record the delivery: ") + ex.what();
                continue;
            }
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

void wake_current() {
    if (auto c = load_slot(current()))
        c->wake();
}

} // namespace

void sync_timer_queues() {
    // every space's maintenance thread calls this; once a poll interval is enough
    static std::atomic<int64_t> last_ms{0};
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now().time_since_epoch()).count();
    int64_t was = last_ms.load();
    if (now - was < (int64_t) barch::get_maintenance_poll_delay()
        || !last_ms.compare_exchange_strong(was, now))
        return;
    std::vector<std::pair<std::string, std::shared_ptr<queue>>> due;
    {
        std::lock_guard lock(reg().mut);
        for (const auto& [name, q] : reg().queues)
            if (q.on_timer)
                due.emplace_back(name, q.file);
    }
    // outside the registry lock: a sync can take a while, and publishes need it
    for (const auto& [name, file] : due) {
        try {
            file->sync();
        } catch (const std::exception& e) {
            barch::err({"could not sync queue", name, e.what()});
        }
    }
}

void request_rescan() {
    /*
     * Here and now, not only on the consumer's next tick - TODO 516. There may
     * be no consumer (no server yet), and a push right after the declaration
     * changed must already see the change: that's what makes a removed queue
     * refuse it, and a changed user apply from the next delivery on.
     */
    reconcile();
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

/** bytes written to an open queue and not yet synced, read now - TODO 515 */
uint64_t unsynced_of(const std::string& name) {
    std::shared_ptr<queue> file;
    {
        std::lock_guard lock(reg().mut);
        auto found = reg().queues.find(name);
        if (found == reg().queues.end())
            return 0;           // not opened yet, so nothing written
        file = found->second.file;
    }
    return file->unsynced_bytes();
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
          << " unsynced=" << unsynced_of(e.spec.name)
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

namespace {
void stop_consumer(bool wait_for_handlers) {
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
    /*
     * Only at shutdown - TODO 517. A delivery settles itself on the queue now,
     * so nothing is lost by not waiting, and server::start() used to spin here
     * for up to 10 s holding srv_mut whenever a handler was parked on I/O. At
     * shutdown the wait still gives a handler that's nearly done the chance
     * to finish before the process goes.
     */
    for (int i = 0; wait_for_handlers && i < 10000 && c->pending.load() > 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    if (c->pending.load() > 0) {
        c->abandoned.store(true);
        if (wait_for_handlers)
            barch::err({"queue handlers still outstanding at stop", c->pending.load()});
        else
            barch::log({"queue consumer replaced with", c->pending.load(),
                        "handlers or ticks still out - they settle on their own"});
        new std::shared_ptr<consumer>(c); // deliberately never freed
    }
    /*
     * The files stay open. A publish is allowed with no consumer armed - that is
     * what makes a message written during a load durable - and closing them here
     * would turn a stop into a reason for a publish to fail.
     */
}
} // namespace

void stop() {
    stop_consumer(true);
}

void stop_no_wait() {
    stop_consumer(false);
}

}
}
