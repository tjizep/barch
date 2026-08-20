// Isolated tests for src/debuggable_server_lock.h.
//
// Correctness against overlapping readers/writers, timeouts, and the
// std::unique_lock / std::shared_lock surface latch_t needs. Then a
// read-heavy throughput check against std::shared_timed_mutex so a
// lock that silently serializes readers fails rather than looking fast.

#include "debuggable_server_lock.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

std::atomic<int> failed{0};

void expect(bool ok, const char* what) {
    if (ok)
        return;
    ++failed;
    std::fprintf(stderr, "FAIL: %s\n", what);
}

template<typename Lock>
void run_mix(Lock& lk, int readers, int writers, int ms) {
    std::atomic<int> readers_in{0};
    std::atomic<int> writers_in{0};
    std::atomic<int> overlap{0};
    std::atomic<bool> stop{false};
    std::vector<std::thread> ts;

    for (int i = 0; i < readers; ++i) {
        ts.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                lk.lock_shared();
                int w = writers_in.load(std::memory_order_relaxed);
                readers_in.fetch_add(1, std::memory_order_relaxed);
                if (w != 0)
                    overlap.fetch_add(1, std::memory_order_relaxed);
                std::this_thread::yield();
                readers_in.fetch_sub(1, std::memory_order_relaxed);
                lk.unlock_shared();
            }
        });
    }
    for (int i = 0; i < writers; ++i) {
        ts.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                lk.lock();
                int w = writers_in.fetch_add(1, std::memory_order_relaxed);
                int r = readers_in.load(std::memory_order_relaxed);
                if (w != 0 || r != 0)
                    overlap.fetch_add(1, std::memory_order_relaxed);
                std::this_thread::yield();
                writers_in.fetch_sub(1, std::memory_order_relaxed);
                lk.unlock();
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop.store(true, std::memory_order_relaxed);
    for (auto& t : ts)
        t.join();
    expect(overlap.load() == 0, "reader/writer overlap");
}

template<typename Lock>
uint64_t read_ops(Lock& lk, int threads, int ms) {
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> ops{0};
    std::vector<std::thread> ts;
    for (int i = 0; i < threads; ++i) {
        ts.emplace_back([&] {
            uint64_t n = 0;
            while (!stop.load(std::memory_order_relaxed)) {
                lk.lock_shared();
                lk.unlock_shared();
                ++n;
            }
            ops.fetch_add(n, std::memory_order_relaxed);
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop.store(true, std::memory_order_relaxed);
    for (auto& t : ts)
        t.join();
    return ops.load();
}

void test_shared_exclusive() {
    debuggable_server_lock lk;
    std::atomic<int> seen{0};
    lk.lock_shared();
    std::thread w([&] {
        lk.lock();
        seen.store(1);
        lk.unlock();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    expect(seen.load() == 0, "writer waited on a live reader");
    lk.unlock_shared();
    w.join();
    expect(seen.load() == 1, "writer ran after the reader left");
}

void test_timeout() {
    debuggable_server_lock lk;
    lk.lock();
    auto t0 = std::chrono::steady_clock::now();
    bool got = lk.try_lock_for(std::chrono::milliseconds(20));
    auto dt = std::chrono::steady_clock::now() - t0;
    expect(!got, "try_lock_for failed while the write lock was held");
    expect(dt >= std::chrono::milliseconds(10), "try_lock_for waited");
    bool rgot = lk.try_lock_shared_for(std::chrono::milliseconds(20));
    expect(!rgot, "try_lock_shared_for failed while the write lock was held");
    lk.unlock();
    expect(lk.try_lock_for(std::chrono::milliseconds(20)), "try_lock_for succeeded when free");
    lk.unlock();
}

void test_std_guards() {
    debuggable_server_lock lk;
    {
        std::shared_lock r1(lk);
        std::shared_lock r2(lk);
        expect(!lk.try_lock(), "unique try_lock failed under shared_lock");
    }
    {
        std::unique_lock w(lk);
        expect(!lk.try_lock_shared(), "shared try_lock failed under unique_lock");
        w.unlock();
        expect(lk.try_lock_shared(), "shared try_lock succeeded after unique_lock unlock");
        lk.unlock_shared();
    }
    {
        std::unique_lock w(lk, std::defer_lock);
        expect(w.try_lock_for(std::chrono::milliseconds(20)), "unique_lock::try_lock_for");
    }
}

void test_many_readers() {
    debuggable_server_lock lk;
    constexpr int n = 8;
    std::atomic<int> in{0};
    std::atomic<int> max_in{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> ts;
    for (int i = 0; i < n; ++i) {
        ts.emplace_back([&] {
            while (!go.load(std::memory_order_relaxed))
                std::this_thread::yield();
            lk.lock_shared();
            int now = in.fetch_add(1) + 1;
            int prev = max_in.load();
            while (now > prev && !max_in.compare_exchange_weak(prev, now)) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            in.fetch_sub(1);
            lk.unlock_shared();
        });
    }
    go.store(true);
    for (auto& t : ts)
        t.join();
    expect(max_in.load() >= 2, "more than one reader held the lock at once");
}

void test_nested_shared() {
    debuggable_server_lock lk;
    lk.lock_shared();
    lk.lock_shared();
    lk.unlock_shared();
    lk.unlock_shared();
}

void test_nested_shared_while_writer_waits() {
    // DEPENDS holds g shared, then storage_release nested-shared-locks
    // the same shard. a writer waiting with write_intent must not
    // turn that nest into a self-deadlock.
    debuggable_server_lock lk;
    lk.lock_shared();
    std::atomic<bool> writer_in{false};
    std::thread w([&] {
        lk.lock();
        writer_in.store(true, std::memory_order_relaxed);
        lk.unlock();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    expect(!writer_in.load(), "writer is still waiting on the outer reader");
    expect(lk.try_lock_shared(), "nested shared succeeds while a writer waits");
    lk.unlock_shared();
    expect(!writer_in.load(), "nested unlock did not release the outer reader");
    lk.unlock_shared();
    w.join();
    expect(writer_in.load(), "writer ran after the outer reader left");
}

void test_try_lock_when_free() {
    debuggable_server_lock lk;
    expect(lk.try_lock(), "try_lock succeeds on a free lock");
    expect(!lk.try_lock(), "try_lock fails while already held");
    expect(!lk.try_lock_shared(), "try_lock_shared fails while uniquely held");
    lk.unlock();
    expect(lk.try_lock_shared(), "try_lock_shared succeeds after unlock");
    lk.unlock_shared();
}

void test_lock_keeps_write_intent() {
    // a live reader, a blocked lock(), and a third thread that must not
    // sneak in as a reader while the writer is waiting. that is the
    // write_intent contract lock() used to break every 15s.
    debuggable_server_lock lk;
    lk.lock_shared();
    std::atomic<bool> writer_in{false};
    std::thread w([&] {
        lk.lock();
        writer_in.store(true, std::memory_order_relaxed);
        lk.unlock();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    expect(!writer_in.load(), "writer is still waiting on the live reader");
    std::atomic<bool> sneaked{false};
    std::thread r2([&] {
        bool got = lk.try_lock_shared();
        sneaked.store(got, std::memory_order_relaxed);
        if (got)
            lk.unlock_shared();
    });
    r2.join();
    expect(!sneaked.load(), "a different thread is refused while a writer waits");
    lk.unlock_shared();
    w.join();
    expect(writer_in.load(), "writer ran after the reader left");
}

void test_lock_wins_reader_stream() {
    debuggable_server_lock lk;
    std::atomic<bool> stop{false};
    std::atomic<int> readers_in{0};
    std::vector<std::thread> ts;
    for (int i = 0; i < 4; ++i) {
        ts.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                if (lk.try_lock_shared()) {
                    readers_in.fetch_add(1, std::memory_order_relaxed);
                    std::this_thread::yield();
                    readers_in.fetch_sub(1, std::memory_order_relaxed);
                    lk.unlock_shared();
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }
    std::atomic<bool> writer_got{false};
    std::thread w([&] {
        lk.lock();
        expect(readers_in.load() == 0, "writer is exclusive against the reader stream");
        writer_got.store(true, std::memory_order_relaxed);
        lk.unlock();
    });
    auto t0 = std::chrono::steady_clock::now();
    while (!writer_got.load(std::memory_order_relaxed) &&
           std::chrono::steady_clock::now() - t0 < std::chrono::seconds(2)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    stop.store(true, std::memory_order_relaxed);
    w.join();
    for (auto& t : ts)
        t.join();
    expect(writer_got.load(), "lock() acquired under a reader stream");
}

void test_snapshot() {
    debuggable_server_lock a, b;
    a.set_label("space#3");
    b.set_label("space#7");
    a.lock_shared();
    b.lock();
    std::string snap = b.debug_snapshot("unit-test");
    expect(snap.find("space#7") != std::string::npos, "snapshot names the lock");
    expect(snap.find("waiter tid:") != std::string::npos, "snapshot has waiter tid");
    expect(snap.find("W  space#7") != std::string::npos, "snapshot lists the write hold");
    expect(snap.find("R  space#3") != std::string::npos, "snapshot lists the earlier read hold");
    expect(snap.find("writer tid:") != std::string::npos, "snapshot has writer tid");
    b.unlock();
    a.unlock_shared();
    std::string after = a.debug_snapshot("unit-test-free");
    expect(after.find("(nothing)") != std::string::npos, "held list empty after unlock");
}

void test_upgradable_allows_readers() {
    debuggable_server_lock lk;
    lk.lock_upgradable();
    std::atomic<int> seen{0};
    std::thread r([&] {
        lk.lock_shared();
        seen.store(1, std::memory_order_relaxed);
        lk.unlock_shared();
    });
    r.join();
    expect(seen.load() == 1, "readers run while a thread holds upgradable");
    lk.unlock_upgrade_without_writing();
}

void test_writer_blocked_during_upgradable() {
    debuggable_server_lock lk;
    lk.lock_upgradable();
    std::atomic<bool> writer_in{false};
    std::thread w([&] {
        lk.lock();
        writer_in.store(true, std::memory_order_relaxed);
        lk.unlock();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    expect(!writer_in.load(), "unique waiter cannot replace data during upgradable");
    lk.unlock_upgrade_without_writing();
    w.join();
    expect(writer_in.load(), "unique ran after upgradable was dropped");
}

void test_upgrade_to_write_excludes_readers() {
    debuggable_server_lock lk;
    lk.lock_upgradable();
    expect(lk.try_upgrade_to_write_for(std::chrono::milliseconds(50)),
           "upgrade to unique with no other readers");
    expect(!lk.try_lock_shared(), "readers refused after upgrade to unique");
    lk.unlock();
}

void test_upgrade_from_shared() {
    debuggable_server_lock lk;
    lk.lock_shared();
    std::atomic<int> other{0};
    std::thread r([&] {
        lk.lock_shared();
        other.store(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        other.store(2, std::memory_order_relaxed);
        lk.unlock_shared();
    });
    while (other.load() == 0)
        std::this_thread::yield();
    expect(lk.try_upgrade_to_write_for(std::chrono::seconds(1)),
           "shared upgraded to unique after the other reader left");
    expect(other.load() == 2, "the other reader finished before upgrade");
    expect(!lk.try_lock_shared(), "readers refused after upgrade from shared");
    lk.unlock();
    r.join();
}

void test_upgrade_from_shared_does_not_deadlock_with_writer() {
    // unique already waiting to drain us: try_upgrade must fail, not wait
    // on the mutex we are keeping busy as a reader.
    debuggable_server_lock lk;
    lk.lock_shared();
    std::atomic<bool> writer_in{false};
    std::thread w([&] {
        lk.lock();
        writer_in.store(true, std::memory_order_relaxed);
        lk.unlock();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    expect(!lk.try_upgrade_to_write_for(std::chrono::milliseconds(20)),
           "upgrade from shared fails while a unique waiter holds the mutex");
    expect(!writer_in.load(), "unique still waiting on our reader");
    lk.unlock_shared();
    w.join();
    expect(writer_in.load(), "unique ran after the reader unlocked");
}

void test_perf() {
    constexpr int ms = 400;
    debuggable_server_lock ours;
    std::shared_timed_mutex stdm;

    uint64_t o1 = read_ops(ours, 1, ms);
    uint64_t o4 = read_ops(ours, 4, ms);
    uint64_t s1 = read_ops(stdm, 1, ms);
    uint64_t s4 = read_ops(stdm, 4, ms);

    std::printf("read ops in %d ms\n", ms);
    std::printf("  ours  1 thread: %llu\n", (unsigned long long)o1);
    std::printf("  ours  4 threads: %llu (%.2fx vs 1)\n",
                (unsigned long long)o4, o1 ? (double)o4 / (double)o1 : 0.0);
    std::printf("  std   1 thread: %llu\n", (unsigned long long)s1);
    std::printf("  std   4 threads: %llu (%.2fx vs 1)\n",
                (unsigned long long)s4, s1 ? (double)s4 / (double)s1 : 0.0);
    if (s4)
        std::printf("  ours/std at 4 threads: %.2fx\n", (double)o4 / (double)s4);

    expect(o4 > o1, "four readers beat one reader (shared, not exclusive)");
    // a 50x hole against the standard lock is a bug, not "a bit slower"
    if (s4 > 0)
        expect(o4 * 50 > s4, "four-reader throughput is within 50x of shared_timed_mutex");
}

}

int main() {
    test_shared_exclusive();
    test_timeout();
    test_try_lock_when_free();
    test_std_guards();
    test_many_readers();
    test_nested_shared();
    test_nested_shared_while_writer_waits();
    test_lock_keeps_write_intent();
    test_lock_wins_reader_stream();
    test_upgradable_allows_readers();
    test_writer_blocked_during_upgradable();
    test_upgrade_to_write_excludes_readers();
    test_upgrade_from_shared();
    test_upgrade_from_shared_does_not_deadlock_with_writer();
#ifdef BARCH_LOCK_DEBUG
    test_snapshot();
#endif

    debuggable_server_lock mix;
    run_mix(mix, 6, 2, 300);

    test_perf();

    if (failed.load()) {
        std::fprintf(stderr, "%d check(s) failed\n", failed.load());
        return 1;
    }
    std::printf("ok\n");
    return 0;
}
