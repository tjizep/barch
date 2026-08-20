//
// Created by test on 8/20/26.
//

#ifndef BARCH_DEBUGGABLE_SERVER_LOCK_H
#define BARCH_DEBUGGABLE_SERVER_LOCK_H
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#ifdef BARCH_LOCK_DEBUG
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#ifdef __linux__
#include <unistd.h>
#include <sys/syscall.h>
#ifdef __GLIBC__
#include <execinfo.h>
#endif
#endif
#if __has_include(<stacktrace>) && __cplusplus >= 202302L
#include <stacktrace>
#define HAS_NATIVE_STACKTRACE 1
#endif
#endif

struct alignas(64) CoreReaderSlot {
    std::atomic<int32_t> reader_count{0};
#ifdef BARCH_LOCK_DEBUG
    std::atomic<uint32_t> last_tid{0};
#endif
};

#ifdef BARCH_LOCK_DEBUG
// Write-path diagnostics only. Not over-aligned: shards are malloc'd.
struct LockDiagnostics {
    std::atomic<std::thread::id> active_writer_id{};
    std::atomic<std::thread::id> active_upgrader_id{};
    std::atomic<uint32_t> writer_tid{0};
    std::atomic<int64_t> writer_since_ns{0};
    static constexpr int max_frames = 24;
    void* writer_frames[max_frames]{};
    std::atomic<int> writer_nframes{0};
};
#endif

/**
 * Reader/writer lock with per-thread reader slots. Upgradable holds are
 * readers plus the exclusive right to become unique later, so compress
 * can run without blocking other readers and without letting a writer
 * replace the value. Timeout dumps, labels, and writer backtraces are
 * compiled in when BARCH_LOCK_DEBUG is defined.
 */
class debuggable_server_lock {
public:
    // DEPENDS takes every shard of two spaces, then storage_release
    // nested-shared-locks the source again. 16 hid most of the list.
    static constexpr int max_held = 2048;
#ifdef BARCH_LOCK_DEBUG
    static constexpr int label_cap = 80;
    static constexpr auto dump_every = std::chrono::seconds(15);
#endif

    struct hold_rec {
        const debuggable_server_lock* lk;
        char mode;
        int rec;
    };

private:
    std::vector<CoreReaderSlot> core_slots;
    std::timed_mutex upgrade_write_mtx;
    std::atomic<bool> write_intent{false};

    std::mutex cv_mtx;
    std::condition_variable cv;
    size_t num_cores{1};
    mutable std::atomic<size_t> slot_ticket{0};

#ifdef BARCH_LOCK_DEBUG
    char label_[label_cap]{"(unnamed)"};
    LockDiagnostics diags;
    static inline std::atomic<uint32_t> dump_seq{0};
#endif

    static inline thread_local int tls_slot = -1;
    static inline thread_local hold_rec held[max_held];
    static inline thread_local int held_n = 0;

    size_t slot() const noexcept {
        int s = tls_slot;
        if (s < 0) {
            s = static_cast<int>(slot_ticket.fetch_add(1, std::memory_order_relaxed) % num_cores);
            tls_slot = s;
        }
        return static_cast<size_t>(s) % num_cores;
    }

    bool holds_this_shared() const noexcept {
        for (int i = held_n - 1; i >= 0; --i) {
            if (held[i].lk == this && (held[i].mode == 'R' || held[i].mode == 'U'))
                return true;
        }
        return false;
    }

    char our_hold() const noexcept {
        for (int i = held_n - 1; i >= 0; --i) {
            if (held[i].lk == this)
                return held[i].mode;
        }
        return 0;
    }

    void strip_our_holds() noexcept {
        int w = 0;
        for (int i = 0; i < held_n; ++i) {
            if (held[i].lk != this)
                held[w++] = held[i];
        }
        held_n = w;
    }

    void set_our_hold(char mode) noexcept {
        strip_our_holds();
        push_hold(mode);
    }

    void bump_reader_hold() noexcept {
        for (int i = held_n - 1; i >= 0; --i) {
            if (held[i].lk == this && (held[i].mode == 'R' || held[i].mode == 'U')) {
                ++held[i].rec;
                return;
            }
        }
        push_hold('R');
    }

    void take_upgradable_reader() noexcept {
        const size_t s = slot();
        core_slots[s].reader_count.fetch_add(1, std::memory_order_seq_cst);
#ifdef BARCH_LOCK_DEBUG
        mark_reader(s);
#endif
        set_our_hold('U');
    }

    void push_hold(char mode) noexcept {
        for (int i = held_n - 1; i >= 0; --i) {
            if (held[i].lk == this && held[i].mode == mode) {
                ++held[i].rec;
                return;
            }
        }
        if (held_n >= max_held)
            return;
        held[held_n].lk = this;
        held[held_n].mode = mode;
        held[held_n].rec = 1;
        ++held_n;
    }

    // true when the outer hold is gone and the slot count should drop
    bool pop_hold() noexcept {
        for (int i = held_n - 1; i >= 0; --i) {
            if (held[i].lk != this)
                continue;
            if (--held[i].rec > 0)
                return false;
            for (int j = i; j < held_n - 1; ++j)
                held[j] = held[j + 1];
            --held_n;
            return true;
        }
        return true;
    }

    bool readers_drained() const noexcept {
        for (size_t i = 0; i < num_cores; ++i) {
            if (core_slots[i].reader_count.load(std::memory_order_seq_cst) != 0)
                return false;
        }
        return true;
    }

    void backoff_reader(size_t s) noexcept {
        int32_t remaining = core_slots[s].reader_count.fetch_sub(1, std::memory_order_seq_cst) - 1;
        if (remaining == 0 && write_intent.load(std::memory_order_seq_cst)) {
            std::lock_guard<std::mutex> lock(cv_mtx);
            cv.notify_all();
        }
    }

#ifdef BARCH_LOCK_DEBUG
    static uint32_t native_tid() noexcept {
        static thread_local uint32_t cached = 0;
        if (cached)
            return cached;
#ifdef __linux__
        cached = static_cast<uint32_t>(syscall(SYS_gettid));
#else
        cached = static_cast<uint32_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
#endif
        return cached;
    }

    static int64_t now_ns() noexcept {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    static void fmt_ns(std::ostream& os, int64_t ns) {
        if (ns <= 0) {
            os << "n/a";
            return;
        }
        os << (ns / 1000000) << " ms";
    }

    void capture_writer_stack() noexcept {
        diags.writer_nframes.store(0, std::memory_order_relaxed);
#if defined(__linux__) && defined(__GLIBC__)
        int n = backtrace(diags.writer_frames, LockDiagnostics::max_frames);
        diags.writer_nframes.store(n, std::memory_order_relaxed);
#endif
    }

    void note_writer() noexcept {
        diags.active_writer_id.store(std::this_thread::get_id(), std::memory_order_relaxed);
        diags.writer_tid.store(native_tid(), std::memory_order_relaxed);
        diags.writer_since_ns.store(now_ns(), std::memory_order_relaxed);
        capture_writer_stack();
    }

    void clear_writer() noexcept {
        diags.active_writer_id.store(std::thread::id(), std::memory_order_relaxed);
        diags.writer_tid.store(0, std::memory_order_relaxed);
        diags.writer_since_ns.store(0, std::memory_order_relaxed);
        diags.writer_nframes.store(0, std::memory_order_relaxed);
    }

    void mark_reader(size_t s) noexcept {
        core_slots[s].last_tid.store(native_tid(), std::memory_order_relaxed);
    }

    void append_waiter_stack(std::ostream& os) const {
#ifdef HAS_NATIVE_STACKTRACE
        os << "--- waiter stack (C++23) ---\n" << std::stacktrace::current() << "\n";
#elif defined(__linux__) && defined(__GLIBC__)
        void* frames[LockDiagnostics::max_frames];
        int n = backtrace(frames, LockDiagnostics::max_frames);
        os << "--- waiter stack ---\n";
        if (char** syms = backtrace_symbols(frames, n)) {
            for (int i = 0; i < n; ++i)
                os << "  " << syms[i] << "\n";
            std::free(syms);
        } else {
            os << "  (backtrace_symbols failed, " << n << " frames)\n";
        }
#else
        os << "--- waiter stack unavailable ---\n";
#endif
    }

    void append_writer_stack(std::ostream& os) const {
        int n = diags.writer_nframes.load(std::memory_order_relaxed);
        os << "--- writer stack at acquire ---\n";
        if (n <= 0) {
            os << "  (none captured)\n";
            return;
        }
#if defined(__linux__) && defined(__GLIBC__)
        if (char** syms = backtrace_symbols(const_cast<void**>(diags.writer_frames), n)) {
            for (int i = 0; i < n; ++i)
                os << "  " << syms[i] << "\n";
            std::free(syms);
            return;
        }
#endif
        for (int i = 0; i < n; ++i)
            os << "  " << diags.writer_frames[i] << "\n";
    }

    void emit_dump(const std::string& text) const {
        std::cerr << text << std::flush;
        uint32_t seq = dump_seq.fetch_add(1, std::memory_order_relaxed) + 1;
        char path[160];
        std::snprintf(path, sizeof(path), "barch-lock-timeout-%u-%u-%u.txt",
                      static_cast<unsigned>(
#ifdef __linux__
                          getpid()
#else
                          0
#endif
                              ),
                      native_tid(), seq);
        if (FILE* f = std::fopen(path, "w")) {
            std::fwrite(text.data(), 1, text.size(), f);
            std::fclose(f);
            std::cerr << "[lock timeout dump also written to " << path << "]\n" << std::flush;
        }
    }

    template <typename Rep, typename Period>
    void log_if_slow_timeout(const std::chrono::duration<Rep, Period>& timeout_duration,
                             const char* phase, int64_t wait_start_ns) {
        if (timeout_duration < std::chrono::seconds(1))
            return;
        emit_dump(debug_snapshot(phase, wait_start_ns));
    }
#endif

public:
    explicit debuggable_server_lock(size_t /*hierarchy_id*/ = 0) {
        num_cores = std::thread::hardware_concurrency();
        if (num_cores == 0)
            num_cores = 1;
        core_slots = std::vector<CoreReaderSlot>(num_cores);
    }

    debuggable_server_lock(const debuggable_server_lock&) = delete;
    debuggable_server_lock& operator=(const debuggable_server_lock&) = delete;

#ifdef BARCH_LOCK_DEBUG
    void set_label(std::string_view s) noexcept {
        size_t n = s.size();
        if (n >= label_cap)
            n = label_cap - 1;
        std::memcpy(label_, s.data(), n);
        label_[n] = 0;
    }

    const char* label() const noexcept { return label_; }

    std::string debug_snapshot(const char* phase = "snapshot", int64_t wait_start_ns = 0) const {
        int64_t now = now_ns();
        std::stringstream ss;
        ss << "\n==================================================\n";
        ss << "[LOCK TIMEOUT CRITICAL] " << phase << "\n";
        ss << "lock: " << label_ << "  this=" << static_cast<const void*>(this) << "\n";
        ss << "waiter tid: " << native_tid() << "  thread::id: " << std::this_thread::get_id() << "\n";
        if (wait_start_ns) {
            ss << "waiter waited: ";
            fmt_ns(ss, now - wait_start_ns);
            ss << "\n";
        }
        ss << "write_intent: " << write_intent.load(std::memory_order_relaxed) << "\n";

        auto wid = diags.active_writer_id.load();
        uint32_t wtid = diags.writer_tid.load(std::memory_order_relaxed);
        int64_t wsince = diags.writer_since_ns.load(std::memory_order_relaxed);
        if (wtid || wid != std::thread::id()) {
            ss << "writer tid: " << wtid << "  thread::id: " << wid << "  held for: ";
            fmt_ns(ss, wsince ? now - wsince : 0);
            ss << "\n";
        } else {
            ss << "writer: none\n";
        }
        auto uid = diags.active_upgrader_id.load();
        if (uid != std::thread::id())
            ss << "upgrader thread::id: " << uid << "\n";

        ss << "this thread already holds:\n";
        if (held_n == 0)
            ss << "  (nothing)\n";
        for (int i = 0; i < held_n; ++i) {
            const auto* h = held[i].lk;
            ss << "  " << held[i].mode << "  " << (h ? h->label() : "?")
               << "  " << static_cast<const void*>(h);
            if (held[i].rec > 1)
                ss << "  rec=" << held[i].rec;
            ss << "\n";
        }
        if (held_n >= max_held)
            ss << "  (held list full, further locks were not recorded)\n";

        ss << "reader slots (count, last tid):\n";
        bool any = false;
        for (size_t i = 0; i < num_cores; ++i) {
            int32_t c = core_slots[i].reader_count.load(std::memory_order_relaxed);
            uint32_t tid = core_slots[i].last_tid.load(std::memory_order_relaxed);
            if (c == 0 && tid == 0)
                continue;
            any = true;
            ss << "  slot " << i << ": count=" << c << " last_tid=" << tid << "\n";
        }
        if (!any)
            ss << "  (all zero)\n";

        append_writer_stack(ss);
        append_waiter_stack(ss);
        ss << "==================================================\n";
        return ss.str();
    }
#else
    void set_label(std::string_view) noexcept {}
    const char* label() const noexcept { return ""; }
    std::string debug_snapshot(const char* = "snapshot", int64_t = 0) const { return {}; }
#endif

    // --- READ PATH ---
    template <typename Rep, typename Period>
    bool try_lock_shared_for(const std::chrono::duration<Rep, Period>& timeout_duration) {
        // nested shared on a lock this thread already holds must not wait
        // on write_intent: that is the DEPENDS / storage_release self-deadlock.
        if (holds_this_shared()) {
            bump_reader_hold();
            return true;
        }

        const size_t s = slot();
        auto deadline = std::chrono::steady_clock::now() + timeout_duration;
#ifdef BARCH_LOCK_DEBUG
        int64_t started = now_ns();
#endif

        while (true) {
            core_slots[s].reader_count.fetch_add(1, std::memory_order_seq_cst);
#ifdef BARCH_LOCK_DEBUG
            mark_reader(s);
#endif

            if (!write_intent.load(std::memory_order_seq_cst)) {
                push_hold('R');
                return true;
            }

            backoff_reader(s);

            auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
#ifdef BARCH_LOCK_DEBUG
                log_if_slow_timeout(timeout_duration, "Shared Read Acquisition", started);
#endif
                return false;
            }
#ifdef BARCH_LOCK_DEBUG
            auto slice = now + dump_every;
            if (slice > deadline)
                slice = deadline;
#else
            auto slice = deadline;
#endif

            std::unique_lock<std::mutex> lock(cv_mtx);
            if (!cv.wait_until(lock, slice, [this] {
                    return !write_intent.load(std::memory_order_seq_cst);
                })) {
                if (std::chrono::steady_clock::now() >= deadline) {
#ifdef BARCH_LOCK_DEBUG
                    log_if_slow_timeout(timeout_duration, "Shared Read Acquisition", started);
#endif
                    return false;
                }
#ifdef BARCH_LOCK_DEBUG
                log_if_slow_timeout(dump_every, "Shared Read Acquisition", started);
#endif
            }
        }
    }

    template <class Clock, class Duration>
    bool try_lock_shared_until(const std::chrono::time_point<Clock, Duration>& abs) {
        auto now = Clock::now();
        if (abs <= now)
            return try_lock_shared();
        return try_lock_shared_for(abs - now);
    }

    bool try_lock_shared() {
        return try_lock_shared_for(std::chrono::nanoseconds(0));
    }

    void lock_shared() {
        if (holds_this_shared()) {
            bump_reader_hold();
            return;
        }
        const size_t s = slot();
        while (true) {
            core_slots[s].reader_count.fetch_add(1, std::memory_order_seq_cst);
#ifdef BARCH_LOCK_DEBUG
            mark_reader(s);
#endif
            if (!write_intent.load(std::memory_order_seq_cst)) {
                push_hold('R');
                return;
            }
            backoff_reader(s);
            std::unique_lock<std::mutex> lock(cv_mtx);
#ifdef BARCH_LOCK_DEBUG
            int64_t started = now_ns();
            if (!cv.wait_for(lock, dump_every, [this] {
                    return !write_intent.load(std::memory_order_seq_cst);
                })) {
                log_if_slow_timeout(dump_every, "Blocking Shared Read", started);
            }
#else
            cv.wait(lock, [this] { return !write_intent.load(std::memory_order_seq_cst); });
#endif
        }
    }

    void unlock_shared() noexcept {
        if (pop_hold())
            backoff_reader(slot());
    }

    // --- WRITE PATH ---
    template <typename Rep, typename Period>
    bool try_lock_for(const std::chrono::duration<Rep, Period>& timeout_duration) {
        auto deadline = std::chrono::steady_clock::now() + timeout_duration;
#ifdef BARCH_LOCK_DEBUG
        int64_t started = now_ns();
#endif

        std::unique_lock<std::timed_mutex> lock(upgrade_write_mtx, std::defer_lock);
        while (true) {
            auto now = std::chrono::steady_clock::now();
#ifdef BARCH_LOCK_DEBUG
            auto slice = now + dump_every;
            if (slice > deadline)
                slice = deadline;
#else
            auto slice = deadline;
#endif
            // try at least once, including a zero timeout (try_lock_until(now)
            // is try_lock). checking the deadline first made try_lock() always fail.
            if (lock.try_lock_until(slice))
                break;
            if (std::chrono::steady_clock::now() >= deadline) {
#ifdef BARCH_LOCK_DEBUG
                log_if_slow_timeout(timeout_duration, "Write Mutex Contention", started);
#endif
                return false;
            }
#ifdef BARCH_LOCK_DEBUG
            log_if_slow_timeout(dump_every, "Write Mutex Contention", started);
#endif
        }

        write_intent.store(true, std::memory_order_seq_cst);

        std::unique_lock<std::mutex> cv_lock(cv_mtx);
        while (!readers_drained()) {
            auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                write_intent.store(false, std::memory_order_seq_cst);
                cv.notify_all();
#ifdef BARCH_LOCK_DEBUG
                log_if_slow_timeout(timeout_duration, "Draining Readers During Write", started);
#endif
                return false;
            }
#ifdef BARCH_LOCK_DEBUG
            auto slice = now + dump_every;
            if (slice > deadline)
                slice = deadline;
            if (!cv.wait_until(cv_lock, slice, [this] { return readers_drained(); })) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    write_intent.store(false, std::memory_order_seq_cst);
                    cv.notify_all();
                    log_if_slow_timeout(timeout_duration, "Draining Readers During Write", started);
                    return false;
                }
                log_if_slow_timeout(dump_every, "Draining Readers During Write", started);
            }
#else
            if (!cv.wait_until(cv_lock, deadline, [this] { return readers_drained(); })) {
                write_intent.store(false, std::memory_order_seq_cst);
                cv.notify_all();
                return false;
            }
#endif
        }

#ifdef BARCH_LOCK_DEBUG
        note_writer();
#endif
        push_hold('W');
        lock.release();
        return true;
    }

    template <class Clock, class Duration>
    bool try_lock_until(const std::chrono::time_point<Clock, Duration>& abs) {
        auto now = Clock::now();
        if (abs <= now)
            return try_lock();
        return try_lock_for(abs - now);
    }

    bool try_lock() {
        return try_lock_for(std::chrono::nanoseconds(0));
    }

    void lock() {
#ifdef BARCH_LOCK_DEBUG
        // keep write_intent and the upgrade mutex across dumps. looping
        // try_lock_for() used to drop both every 15s, so readers flooded
        // back in and the writer never drained.
        int64_t started = now_ns();
        std::unique_lock<std::timed_mutex> ul(upgrade_write_mtx, std::defer_lock);
        while (!ul.try_lock_for(dump_every)) {
            log_if_slow_timeout(dump_every, "Write Mutex Contention", started);
        }

        write_intent.store(true, std::memory_order_seq_cst);

        {
            std::unique_lock<std::mutex> cv_lock(cv_mtx);
            while (!readers_drained()) {
                if (!cv.wait_for(cv_lock, dump_every, [this] { return readers_drained(); })) {
                    log_if_slow_timeout(dump_every, "Draining Readers During Write", started);
                }
            }
        }

        note_writer();
        push_hold('W');
        ul.release();
#else
        std::unique_lock<std::timed_mutex> ul(upgrade_write_mtx);
        write_intent.store(true, std::memory_order_seq_cst);
        {
            std::unique_lock<std::mutex> cv_lock(cv_mtx);
            cv.wait(cv_lock, [this] { return readers_drained(); });
        }
        push_hold('W');
        ul.release();
#endif
    }

    void unlock() noexcept {
        pop_hold();
#ifdef BARCH_LOCK_DEBUG
        clear_writer();
#endif
        write_intent.store(false, std::memory_order_seq_cst);
        {
            std::lock_guard<std::mutex> lock(cv_mtx);
            cv.notify_all();
        }
        upgrade_write_mtx.unlock();
    }

    // Upgradable is a reader plus the exclusive right to become unique later.
    // Other readers continue. Writers wait on the mutex, so the value cannot
    // be replaced during compress. write_intent is not set until upgrade.
    template <typename Rep, typename Period>
    bool try_lock_upgradable_for(const std::chrono::duration<Rep, Period>& timeout_duration) {
        char have = our_hold();
        if (have == 'U' || have == 'W')
            return true;

#ifdef BARCH_LOCK_DEBUG
        int64_t started = now_ns();
#endif
        std::unique_lock<std::timed_mutex> lock(upgrade_write_mtx, std::defer_lock);
        // already a shared reader: must not block on the mutex. a unique
        // waiter already holds it and is draining us — waiting would deadlock.
        if (have == 'R') {
            if (!lock.try_lock())
                return false;
            set_our_hold('U');
#ifdef BARCH_LOCK_DEBUG
            diags.active_upgrader_id.store(std::this_thread::get_id(), std::memory_order_relaxed);
#endif
            lock.release();
            return true;
        }

        if (!lock.try_lock_for(timeout_duration)) {
#ifdef BARCH_LOCK_DEBUG
            log_if_slow_timeout(timeout_duration, "Upgrade Mutex Contention", started);
#endif
            return false;
        }
        take_upgradable_reader();
#ifdef BARCH_LOCK_DEBUG
        diags.active_upgrader_id.store(std::this_thread::get_id(), std::memory_order_relaxed);
#endif
        lock.release();
        return true;
    }

    void lock_upgradable() {
        if (our_hold() == 'U' || our_hold() == 'W')
            return;
        // already shared: only try_lock the mutex. a unique waiter may
        // already hold it and be draining us.
        if (our_hold() == 'R') {
            try_lock_upgradable_for(std::chrono::nanoseconds(0));
            return;
        }
        std::unique_lock<std::timed_mutex> lock(upgrade_write_mtx);
        take_upgradable_reader();
#ifdef BARCH_LOCK_DEBUG
        diags.active_upgrader_id.store(std::this_thread::get_id(), std::memory_order_relaxed);
#endif
        lock.release();
    }

    template <typename Rep, typename Period>
    bool try_upgrade_to_write_for(const std::chrono::duration<Rep, Period>& timeout_duration) {
        char have = our_hold();
        if (have == 'W')
            return true;
        if (have != 'U' && have != 'R')
            return false;

        auto deadline = std::chrono::steady_clock::now() + timeout_duration;
#ifdef BARCH_LOCK_DEBUG
        int64_t started = now_ns();
#endif

        if (have == 'R') {
            std::unique_lock<std::timed_mutex> lock(upgrade_write_mtx, std::defer_lock);
            if (!lock.try_lock_until(deadline)) {
#ifdef BARCH_LOCK_DEBUG
                log_if_slow_timeout(timeout_duration, "Upgrade Mutex Contention", started);
#endif
                return false;
            }
            set_our_hold('U');
#ifdef BARCH_LOCK_DEBUG
            diags.active_upgrader_id.store(std::this_thread::get_id(), std::memory_order_relaxed);
#endif
            lock.release();
        }

        write_intent.store(true, std::memory_order_seq_cst);
        backoff_reader(slot());

        std::unique_lock<std::mutex> cv_lock(cv_mtx);
        while (!readers_drained()) {
            auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                write_intent.store(false, std::memory_order_seq_cst);
                cv.notify_all();
                cv_lock.unlock();
                // still upgradable: put our reader count back
                const size_t s = slot();
                core_slots[s].reader_count.fetch_add(1, std::memory_order_seq_cst);
#ifdef BARCH_LOCK_DEBUG
                mark_reader(s);
                log_if_slow_timeout(timeout_duration, "Draining Readers During Upgrade Escalation", started);
#endif
                return false;
            }
#ifdef BARCH_LOCK_DEBUG
            auto slice = now + dump_every;
            if (slice > deadline)
                slice = deadline;
            if (!cv.wait_until(cv_lock, slice, [this] { return readers_drained(); })) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    write_intent.store(false, std::memory_order_seq_cst);
                    cv.notify_all();
                    cv_lock.unlock();
                    const size_t s = slot();
                    core_slots[s].reader_count.fetch_add(1, std::memory_order_seq_cst);
                    mark_reader(s);
                    log_if_slow_timeout(timeout_duration, "Draining Readers During Upgrade Escalation", started);
                    return false;
                }
                log_if_slow_timeout(dump_every, "Draining Readers During Upgrade Escalation", started);
            }
#else
            if (!cv.wait_until(cv_lock, deadline, [this] { return readers_drained(); })) {
                write_intent.store(false, std::memory_order_seq_cst);
                cv.notify_all();
                cv_lock.unlock();
                core_slots[slot()].reader_count.fetch_add(1, std::memory_order_seq_cst);
                return false;
            }
#endif
        }

#ifdef BARCH_LOCK_DEBUG
        note_writer();
        diags.active_upgrader_id.store(std::thread::id(), std::memory_order_relaxed);
#endif
        set_our_hold('W');
        return true;
    }

    void upgrade_to_write() {
        if (our_hold() == 'W')
            return;
        if (our_hold() == 'R')
            try_lock_upgradable_for(std::chrono::nanoseconds(0));
        if (our_hold() != 'U')
            return;
#ifdef BARCH_LOCK_DEBUG
        int64_t started = now_ns();
#endif
        write_intent.store(true, std::memory_order_seq_cst);
        backoff_reader(slot());
        {
            std::unique_lock<std::mutex> cv_lock(cv_mtx);
#ifdef BARCH_LOCK_DEBUG
            while (!readers_drained()) {
                if (!cv.wait_for(cv_lock, dump_every, [this] { return readers_drained(); }))
                    log_if_slow_timeout(dump_every, "Draining Readers During Upgrade Escalation", started);
            }
#else
            cv.wait(cv_lock, [this] { return readers_drained(); });
#endif
        }
#ifdef BARCH_LOCK_DEBUG
        note_writer();
        diags.active_upgrader_id.store(std::thread::id(), std::memory_order_relaxed);
#endif
        set_our_hold('W');
    }

    void unlock_write() noexcept {
        unlock();
    }

    void unlock_upgrade_without_writing() noexcept {
        pop_hold();
        backoff_reader(slot());
#ifdef BARCH_LOCK_DEBUG
        diags.active_upgrader_id.store(std::thread::id(), std::memory_order_relaxed);
#endif
        {
            std::lock_guard<std::mutex> lock(cv_mtx);
            cv.notify_all();
        }
        upgrade_write_mtx.unlock();
    }
};

#endif //BARCH_DEBUGGABLE_SERVER_LOCK_H
