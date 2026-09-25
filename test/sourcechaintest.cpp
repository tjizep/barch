// locking a dependent shard's source chain lets go of what it took when a lock
// in the chain times out - TODO 453
#include "source_chain.h"
#include <cstdio>
#include <memory>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <vector>

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-58s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

// just enough of a shard: a latch, a source, and a lock_shared that can be told
// to time out the way abstract_shard::lock_shared does
struct fake_shard {
    std::shared_mutex latch;
    std::shared_ptr<fake_shard> src;
    bool times_out = false;
    int held = 0;
    [[nodiscard]] std::shared_ptr<fake_shard> sources() const { return src; }
    void lock_shared() {
        if (times_out) throw std::runtime_error("read lock wait time exceeded");
        latch.lock_shared();
        ++held;
    }
    void unlock_shared() {
        latch.unlock_shared();
        --held;
    }
    // what a writer needs, and what hung when a chain was left held
    bool writable() {
        if (!latch.try_lock()) return false;
        latch.unlock();
        return true;
    }
};
using ptr = std::shared_ptr<fake_shard>;

// a chain of n shards, first to last
static std::vector<ptr> chain(size_t n) {
    std::vector<ptr> out(n);
    for (auto& s : out) s = std::make_shared<fake_shard>();
    for (size_t i = 0; i + 1 < n; ++i) out[i]->src = out[i + 1];
    return out;
}

static bool all_free(const std::vector<ptr>& c) {
    for (auto& s : c)
        if (s->held != 0 || !s->writable()) return false;
    return true;
}

int main() {
    std::printf("no sources\n");
    {
        check(barch::lock_source_chain(ptr{}) == 0, "an empty chain locks nothing");
        barch::unlock_source_chain(ptr{}, 0);
    }

    std::printf("a whole chain\n");
    {
        auto c = chain(3);
        const size_t n = barch::lock_source_chain(c[0]);
        check(n == 3, "three shards locked");
        check(c[0]->held == 1 && c[1]->held == 1 && c[2]->held == 1, "each one once");
        check(!c[2]->writable(), "a writer has to wait for them");
        barch::unlock_source_chain(c[0], n);
        check(all_free(c), "and they're all free after the unlock");
    }

    std::printf("a timeout partway along\n");
    {
        auto c = chain(3);
        c[2]->times_out = true;
        bool threw = false;
        try {
            barch::lock_source_chain(c[0]);
        } catch (const std::runtime_error&) {
            threw = true;
        }
        check(threw, "the timeout still reaches the caller");
        check(all_free(c), "the two taken before it are let go");
    }

    std::printf("a timeout on the first\n");
    {
        auto c = chain(2);
        c[0]->times_out = true;
        bool threw = false;
        try {
            barch::lock_source_chain(c[0]);
        } catch (const std::runtime_error&) {
            threw = true;
        }
        check(threw && all_free(c), "nothing is left held");
    }

    std::printf("the shard's own lock failing after the chain\n");
    {
        // what storage_release and read_lock_t do when their own lock throws
        auto c = chain(2);
        auto self = std::make_shared<fake_shard>();
        self->src = c[0];
        self->times_out = true;
        bool threw = false;
        try {
            const size_t n = barch::lock_source_chain(self->sources());
            try {
                self->lock_shared();
            } catch (...) {
                barch::unlock_source_chain(self->sources(), n);
                throw;
            }
        } catch (const std::runtime_error&) {
            threw = true;
        }
        check(threw && all_free(c), "the sources are let go");
    }

    std::printf("a count shorter than the chain\n");
    {
        auto c = chain(3);
        c[0]->lock_shared();
        barch::unlock_source_chain(c[0], 1);
        check(all_free(c), "only what was counted is unlocked");
    }

    std::printf("\n%s\n", failures == 0 ? "all source chain checks pass" : "FAILURES above");
    return failures == 0 ? 0 : 1;
}
