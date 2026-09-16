// the change log: checkpoints mean saved, so replay starts after the last one
// and a trim drops it along with everything before it - TODO 354
#include "aof_log.h"
#include <cstdio>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

using namespace barch;

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-58s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

int main() {
    const std::string path = "aoflogtest.dat";
    ::unlink(path.c_str());
    const sync_policy on_demand{sync_when::on_demand, 0};

    std::printf("sequences and appends\n");
    {
        aof::log l(path, on_demand);
        check(l.next_sequence() == 1, "a new log starts at sequence 1");
        const uint64_t a = l.append_set("shop", "k1", "v1");
        const uint64_t b = l.append_set("shop", "k2", "v2", 1789500000000ll);
        const uint64_t c = l.append_erase("shop", "k1");
        check(a == 1 && b == 2 && c == 3, "sequences are handed out in order");
        check(l.records() == 3, "three records");
        l.sync();
    }
    {
        aof::log l(path, on_demand);
        check(l.next_sequence() == 4, "reopening carries on from the highest seen");
    }

    std::printf("replay before any checkpoint\n");
    {
        aof::log l(path, on_demand);
        std::vector<aof::record> seen;
        const auto out = l.replay([&](const aof::record& r) { seen.push_back(r); });
        check(out.records == 3 && !out.stopped_early, "everything replays");
        check(seen.size() == 3 && seen[0].key == "k1" && seen[1].key == "k2"
              && seen[2].type == aof::record_type::erase, "in order, with the types kept");
        check(seen[1].expiry_ms == 1789500000000ll, "and the expiry");
    }

    std::printf("a checkpoint means everything before it is saved\n");
    {
        aof::log l(path, on_demand);
        l.checkpoint("shop");
        l.append_set("shop", "k3", "v3");
        l.append_set("shop", "k4", "v4");
        std::vector<aof::record> seen;
        const auto out = l.replay([&](const aof::record& r) { seen.push_back(r); });
        check(out.records == 2, "replay covers only what follows the checkpoint");
        check(seen.size() == 2 && seen[0].key == "k3" && seen[1].key == "k4",
              "which is the two writes after it");
        check(l.records() == 6, "while the log still holds all six records");
    }

    std::printf("trimming to the last checkpoint\n");
    {
        aof::log l(path, on_demand);
        const auto out = l.trim_to_last_checkpoint();
        check(out.records == 4, "the checkpoint and the three before it are dropped");
        check(l.records() == 2, "leaving the two that follow it");
        std::vector<aof::record> seen;
        l.replay([&](const aof::record& r) { seen.push_back(r); });
        check(seen.size() == 2 && seen[0].key == "k3", "and replay is unchanged by it");
    }

    std::printf("two checkpoints: the last one wins\n");
    {
        ::unlink(path.c_str());
        aof::log l(path, on_demand);
        l.append_set("shop", "a", "1");
        l.checkpoint("shop");
        l.append_set("shop", "b", "2");
        l.checkpoint("shop");
        l.append_set("shop", "c", "3");
        std::vector<aof::record> seen;
        l.replay([&](const aof::record& r) { seen.push_back(r); });
        check(seen.size() == 1 && seen[0].key == "c", "replay starts after the later one");
        const auto out = l.trim_to_last_checkpoint();
        check(out.records == 4 && l.records() == 1, "and the trim drops up to it");
    }

    std::printf("nothing is dropped without a checkpoint\n");
    {
        ::unlink(path.c_str());
        aof::log l(path, on_demand);
        l.append_set("shop", "a", "1");
        l.append_set("shop", "b", "2");
        const auto out = l.trim_to_last_checkpoint();
        check(out.records == 0 && l.records() == 2,
              "with nothing known to be saved, nothing goes");
    }

    std::printf("a torn record stops a replay and says why\n");
    {
        ::unlink(path.c_str());
        {
            aof::log l(path, on_demand);
            l.append_set("shop", "good1", "v");
            l.append_set("shop", "good2", "v");
            l.sync();
        }
        {
            // a partial write, the way a crash under a weak durability leaves one
            queue_file q(path, {sync_when::on_demand, 0});
            const std::string garbage = "not a record at all";
            q.add(garbage);
            q.sync();
        }
        aof::log l(path, on_demand);
        std::vector<aof::record> seen;
        const auto out = l.replay([&](const aof::record& r) { seen.push_back(r); });
        check(out.stopped_early, "the replay stops");
        check(out.records == 2 && seen.size() == 2, "after the records that did verify");
        check(out.why != aof::decoded::ok, "with a reason: "
              + std::string(aof::describe(out.why)));
        check(l.next_sequence() == 3, "and the sequence comes from the last good record");
    }

    std::printf("many threads, one log\n");
    {
        ::unlink(path.c_str());
        aof::log l(path, on_demand);
        const int threads = 8, each = 250;
        std::vector<std::thread> workers;
        for (int t = 0; t < threads; ++t) {
            workers.emplace_back([&l, t, each]() {
                for (int i = 0; i < each; ++i)
                    l.append_set("shop", "t" + std::to_string(t) + "k" + std::to_string(i),
                                 std::string((size_t) (i % 50) + 1, 'v'));
            });
        }
        for (auto& w : workers) w.join();
        l.sync();
        check(l.records() == (uint32_t) (threads * each), "every append landed");
        check(l.next_sequence() == (uint64_t) (threads * each) + 1,
              "and each got its own sequence");

        // every record decodes, and no sequence was handed out twice
        std::vector<bool> seen((size_t) threads * each + 2, false);
        int decoded_ok = 0, dupes = 0;
        const auto out = l.replay([&](const aof::record& r) {
            ++decoded_ok;
            if (r.sequence < seen.size()) {
                if (seen[r.sequence]) ++dupes;
                seen[r.sequence] = true;
            }
        });
        check(!out.stopped_early && decoded_ok == threads * each,
              "all of them decode afterwards");
        check(dupes == 0, "with no sequence used twice");
    }

    ::unlink(path.c_str());
    std::printf("\n%s\n", failures == 0 ? "all aof log checks pass" : "FAILURES above");
    return failures == 0 ? 0 : 1;
}
