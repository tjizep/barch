// a shed up lays its leaves down in key order - TODO 964
//
// The balancer moves keys between neighbouring range shards. Going up it takes the
// lower shard's top key each time. Inserted in that order, the shard above laid its
// new leaves down largest key first, and a read in key order ran about twice as slow
// over them. This loads a range sharded space in key order, runs the sweep until
// nothing moves, and checks:
//   - the keys spread over every shard;
//   - every key reads its own value;
//   - walked in key order, a value sits above the one before it on most steps.
#include "key_space.h"
#include "sharded_store.h"
#include "range_index.h"
#include "function_api.h"
#include "foreign/driver.h"
#include "swig_api.h"

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-66s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

static std::string num(size_t i) {
    char b[16];
    std::snprintf(b, sizeof b, "k%06zu", i);
    return b;
}

int main() {
    const auto dir = std::filesystem::current_path() / "rangeshedorder_data";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    std::filesystem::current_path(dir);

    const std::string name = "shedorder";
    const size_t n = 4000, bytes = 1000;
    KeyValue conf("configuration");
    conf.set(name + ".ordered", "1");
    conf.set(name + ".compression", "none");
    conf.set(name + ".shards", "8");
    conf.set(name + ".range_sharded", "1");
    auto space = barch::get_keyspace(name);
    // the maintenance thread's sweep stays out: this one runs the sweep itself
    space->hold_rebalance();
    space->routes().settle();

    auto acc = barch::functions::store_for_owner(space);
    std::string err, v;
    for (size_t i = 0; i < n; ++i) {
        v.assign(bytes, char('a' + i % 26));
        std::memcpy(v.data(), &i, sizeof i);
        acc.set(num(i), v, err);
        if (!err.empty()) {
            check(false, "set: " + err);
            return 1;
        }
    }

    const auto& shards = space->get_shards();
    size_t moved = 0, sweeps = 0;
    for (; sweeps < 1000; ++sweeps) {
        auto r = space->routes().sweep(shards, barch::get_range_shard_budget(),
                                       barch::get_range_shard_tolerance(), shards.size() * 64);
        moved += r.moved;
        if (!r.moved)
            break;
    }
    space->release_rebalance();
    std::printf("an ordered load of %zu values, %zu moved in %zu sweeps\n", n, moved, sweeps);

    size_t used = 0;
    for (const auto& s : shards)
        used += s->get_tree_size() > 0;
    check(used == shards.size(), "the keys spread over every shard (" + std::to_string(used) + ")");

    barch::sharded_store store(space);
    std::vector<const char*> at;
    size_t wrong = 0;
    for (size_t i = 0; i < n; ++i) {
        const auto key = num(i);
        auto k = space->encode_key(art::value_type{key.data(), key.size()});
        const char* p = nullptr;
        store.search(k.get_value(), [&](const art::node_ptr& node) {
            const auto got = node.const_leaf()->get_value();
            size_t was = 0;
            if (got.size == bytes)
                std::memcpy(&was, got.chars(), sizeof was);
            if (got.size != bytes || was != i || got.chars()[bytes - 1] != char('a' + i % 26))
                ++wrong;
            p = got.chars();
        });
        if (!p)
            ++wrong;
        at.push_back(p);
    }
    check(wrong == 0, "every key reads its own value (" + std::to_string(wrong) + " wrong)");

    // A sweep runs more than one cascade, so a shard takes keys from below more than
    // once. The later ones land in slots an earlier shed freed, and those step back.
    // Measured on this load: 2104 steps down with the old shed, 318 with this one, the
    // same on every run.
    size_t down = 0;
    for (size_t i = 1; i < at.size(); ++i)
        down += at[i] < at[i - 1];
    check(down <= n / 5,"in key order a value sits above the last but for " + std::to_string(down) +
                           " of " + std::to_string(n - 1) + " steps");

    std::printf(failures ? "FAILED\n" : "all passed\n");
    return failures ? 1 : 0;
}
