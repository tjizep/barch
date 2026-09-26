// the change log: checkpoints mean saved, so replay starts after the last one
// and a trim drops it along with everything before it - TODO 354
#include "aof_log.h"
#include <cstdio>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>
#include <csignal>
#include <stdexcept>
#include <sys/resource.h>

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

    std::printf("a torn record is cut off as the log opens, and says why - TODO 466\n");
    {
        ::unlink(path.c_str());
        {
            aof::log l(path, on_demand);
            l.append_set("shop", "good1", "v");
            l.append_set("shop", "good2", "v");
            l.sync();
        }
        {
            // a partial write, the way a crash under a weak durability leaves one,
            // and something after it that can't be trusted either
            queue_file q(path, {sync_when::on_demand, 0});
            q.add(std::string("not a record at all"));
            q.add(std::string("nor this"));
            q.sync();
        }
        {
            aof::log l(path, on_demand);
            const auto& cut = l.cut_at_open();
            check(cut.stopped_early, "opening the log finds the bad record");
            check(cut.why != aof::decoded::ok, "with a reason: "
                  + std::string(aof::describe(cut.why)));
            check(cut.records == 2 && cut.at_sequence == 2,
                  "and cuts it and the one after it, past sequence 2");
            check(l.records() == 2, "leaving the records that did verify");
            check(l.next_sequence() == 3, "and the sequence comes from the last good record");
            std::vector<aof::record> seen;
            const auto out = l.replay([&](const aof::record& r) { seen.push_back(r); });
            check(!out.stopped_early && seen.size() == 2, "so the replay reads to the end");
            l.append_set("shop", "after", "v");
            l.sync();
        }
        // what was appended after the cut is where a replay can read it
        aof::log l(path, on_demand);
        check(!l.cut_at_open().stopped_early, "the next open finds nothing to cut");
        std::vector<aof::record> seen;
        const auto out = l.replay([&](const aof::record& r) { seen.push_back(r); });
        check(!out.stopped_early && seen.size() == 3 && seen.back().key == "after",
              "and a write made after the cut replays");
    }

    std::printf("a clear record replays in its place - TODO 478\n");
    {
        ::unlink(path.c_str());
        {
            aof::log l(path, on_demand);
            l.append_set("shop", "before", "v");
            l.append_clear("shop", 2);
            l.append_set("shop", "after", "v");
            l.sync();
        }
        aof::log l(path, on_demand);
        check(!l.cut_at_open().stopped_early, "a log with a clear in it opens whole");
        std::vector<aof::record> seen;
        const auto out = l.replay([&](const aof::record& r) { seen.push_back(r); });
        check(!out.stopped_early && seen.size() == 3, "and replays to the end");
        check(seen.size() == 3 && seen[1].type == aof::record_type::clear
              && seen[1].space == "shop" && seen[1].shard_count == 2,
              "with the clear between the two writes, naming its space");
    }

    std::printf("a checkpoint never covers less than the last one - TODO 479\n");
    {
        ::unlink(path.c_str());
        {
            aof::log l(path, on_demand);
            for (int i = 0; i < 5; ++i)
                l.append_set("shop", "k" + std::to_string(i), "v");
            l.checkpoint("shop", 4);            // a LOAD at 4
            l.checkpoint("shop", 2);            // a SAVE that took its mark at 2
            l.append_set("shop", "after", "v");
        }
        aof::log l(path, on_demand);
        std::vector<aof::record> seen;
        l.replay([&](const aof::record& r) { seen.push_back(r); });
        check(seen.size() == 2 && seen[0].key == "k4" && seen[1].key == "after",
              "the older mark doesn't bring back what the newer one covered");
        l.checkpoint("shop", 1);                // and it holds across a reopen
        seen.clear();
        l.replay([&](const aof::record& r) { seen.push_back(r); });
        check(seen.size() == 2, "nor after the log is opened again");
    }

    std::printf("a record type from a newer build isn't cut off - TODO 478\n");
    {
        ::unlink(path.c_str());
        {
            aof::log l(path, on_demand);
            l.append_set("shop", "a", "v");
            l.append_set("shop", "b", "v");
            l.sync();
        }
        {
            // a whole record, checksum right, of a type this build doesn't know
            aof::record r;
            r.type = aof::record_type::set;
            r.space = "shop";
            r.key = "c";
            r.sequence = 3;
            std::vector<uint8_t> bytes;
            aof::encode(r, bytes);
            bytes[5] = 99;
            const uint32_t crc = aof::crc32c(bytes.data() + 4, bytes.size() - 4);
            for (int i = 0; i < 4; ++i)
                bytes[i] = (uint8_t) (crc >> (8 * i));
            queue_file q(path, {sync_when::on_demand, 0});
            q.add(bytes);
            q.sync();
        }
        bool refused = false;
        try {
            aof::log l(path, on_demand);
        } catch (const std::runtime_error&) {
            refused = true;
        }
        check(refused, "opening it is refused rather than cutting it");
        queue_file q(path, {sync_when::on_demand, 0});
        check(q.size() == 3, "and all three records are still in the file");
    }

    std::printf("a checkpoint after a cut is found, and trims - TODO 466\n");
    {
        ::unlink(path.c_str());
        {
            aof::log l(path, on_demand);
            l.append_set("shop", "before", "v");
            l.sync();
        }
        {
            queue_file q(path, {sync_when::on_demand, 0});
            q.add(std::string("torn"));
            q.sync();
        }
        {
            aof::log l(path, on_demand);
            l.append_set("shop", "saved", "v");
            l.checkpoint("shop");
            l.append_set("shop", "unsaved", "v");
            const auto out = l.trim_to_last_checkpoint();
            check(!out.stopped_early && out.records == 3,
                  "the trim gets past where the bad record was");
        }
        aof::log l(path, on_demand);
        std::vector<aof::record> seen;
        l.replay([&](const aof::record& r) { seen.push_back(r); });
        check(seen.size() == 1 && seen[0].key == "unsaved",
              "and only the write after the checkpoint replays");
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

    // TODO 452: a save notes the mark first, and writes keep landing while the
    // shards are saved one after another
    std::printf("writes made while a save runs survive the trim\n");
    {
        ::unlink(path.c_str());
        {
            aof::log l(path, on_demand);
            l.append_set("shop", "before1", "v");
            l.append_set("shop", "before2", "v");
            const uint64_t mark = l.mark();
            check(mark == 2, "the mark is the last sequence handed out");
            // the save is running: these may be in no shard file
            l.append_set("shop", "during1", "v");
            l.append_erase("shop", "before1");
            l.checkpoint("shop", mark);
            l.append_set("shop", "after", "v");

            std::vector<std::string> seen;
            l.replay([&](const aof::record& r) { seen.push_back(r.key); });
            check(seen == std::vector<std::string>({"during1", "before1", "after"}),
                  "replay covers the writes made during the save, in order");

            const auto out = l.trim_to_last_checkpoint();
            check(out.records == 2, "the trim drops only the two before the mark");
            check(l.records() == 4, "the checkpoint stays behind the writes it doesn't cover");
            seen.clear();
            l.replay([&](const aof::record& r) { seen.push_back(r.key); });
            check(seen.size() == 3 && seen[0] == "during1", "and replay is unchanged by it");
            l.sync();
        }
        {
            aof::log l(path, on_demand);
            std::vector<std::string> seen;
            l.replay([&](const aof::record& r) { seen.push_back(r.key); });
            check(seen.size() == 3 && seen[0] == "during1" && seen[2] == "after",
                  "the same after a restart");
            check(l.next_sequence() == 7, "and the sequence carries on past the checkpoint");

            // the next save with nothing written during it tidies the rest away
            l.checkpoint("shop", l.mark());
            const auto out = l.trim_to_last_checkpoint();
            check(out.records == 5 && l.records() == 0,
                  "a later save drops the old checkpoint and itself");
        }
    }

    std::printf("a checkpoint from before the mark was recorded\n");
    {
        ::unlink(path.c_str());
        {
            aof::log l(path, on_demand);
            l.append_set("shop", "a", "1");
            l.sync();
        }
        {
            // what an older build wrote: a checkpoint with no value
            queue_file q(path, {sync_when::on_demand, 0});
            aof::record r;
            r.type = aof::record_type::checkpoint;
            r.space = "shop";
            r.sequence = 2;
            std::vector<uint8_t> bytes;
            aof::encode(r, bytes);
            q.add(bytes);
            q.sync();
        }
        aof::log l(path, on_demand);
        l.append_set("shop", "b", "2");
        std::vector<std::string> seen;
        l.replay([&](const aof::record& r) { seen.push_back(r.key); });
        check(seen == std::vector<std::string>({"b"}),
              "it still means everything before it");
        const auto out = l.trim_to_last_checkpoint();
        check(out.records == 2 && l.records() == 1, "and the trim takes it and what it covers");
    }

    std::printf("a checkpoint can't cover writes not made yet\n");
    {
        ::unlink(path.c_str());
        aof::log l(path, on_demand);
        l.append_set("shop", "a", "1");
        l.checkpoint("shop", 1000);
        l.append_set("shop", "b", "2");
        std::vector<std::string> seen;
        l.replay([&](const aof::record& r) { seen.push_back(r.key); });
        check(seen == std::vector<std::string>({"b"}), "a write after it still replays");
    }

    /*
     * A reservation holds its room against every other writer - TODO 471.
     *
     * Last, because it lowers this process's file size limit: past it the log
     * can't grow, the way a full disk stops it. The file is filled to just
     * after a doubling, so half of it is free, and most of that is reserved.
     * Another thread then appends until it's refused, and it has to be refused
     * before it reaches the reserved part.
     */
    std::printf("a reservation keeps its room from other writers\n");
    {
        ::unlink(path.c_str());
        const uint64_t limit = 64 * 1024;
        rlimit was{};
        ::getrlimit(RLIMIT_FSIZE, &was);
        std::signal(SIGXFSZ, SIG_IGN);      // a refused write instead of a killed process
        rlimit lowered = was;
        lowered.rlim_cur = limit;
        ::setrlimit(RLIMIT_FSIZE, &lowered);

        const std::string value(1000, 'v');
        {
            aof::log l(path, on_demand);
            int n = 0;
            while (l.file_bytes() < limit && n < 1000)
                l.append_set("shop", "fill" + std::to_string(n++), value);
            check(l.file_bytes() == limit, "the log is at the size limit");
            const uint64_t free = l.free_bytes();
            check(free > 16 * 1024, "with room left in it (" + std::to_string(free) + " bytes)");

            const uint64_t one = aof::log::record_bytes(4, 10, value.size());
            const uint64_t reserve = free - 3 * one;
            uint64_t others = 0, holder = 0;
            bool refused_outright = false;
            {
                aof::log::reservation held(l, reserve);
                check(l.held_bytes() == reserve, "the reservation holds what it asked for");

                // another shard's writer, on another thread
                std::thread other([&] {
                    try {
                        for (int i = 0; i < 1000; ++i) {
                            l.append_set("shop", "othr" + std::to_string(100000 + i), value);
                            ++others;
                        }
                    } catch (const std::exception&) {
                    }
                });
                other.join();
                check(others <= 3, "another thread is refused once only held room is left ("
                                   + std::to_string(others) + " appends got in)");
                check(l.free_bytes() >= l.held_bytes() && l.held_bytes() == reserve,
                      "and the held room is all still there");

                try {
                    aof::log::reservation too_much(l, l.free_bytes() - l.held_bytes() + 1);
                } catch (const std::exception&) {
                    refused_outright = true;
                }
                check(refused_outright, "a second reservation that doesn't fit is refused");
                check(l.held_bytes() == reserve, "and holds nothing");

                // the holder, on this thread, gets every byte it held
                bool holder_refused = false;
                try {
                    while (holder + one <= reserve) {
                        l.append_set("shop", "hold" + std::to_string(100000 + holder), value);
                        holder += one;
                    }
                } catch (const std::exception&) {
                    holder_refused = true;
                }
                check(!holder_refused, "the holder isn't refused anything it held");
                check(holder > 0 && l.file_bytes() == limit,
                      "the holder writes into its room without growing the file");
                check(l.held_bytes() == reserve - holder, "and what it wrote isn't held any more");
            }
            check(l.held_bytes() == 0, "what's left is let go when the reservation ends");

            // the log is full now, and a checkpoint is a record too
            ::setrlimit(RLIMIT_FSIZE, &was);

            // a trim that empties the log keeps the file's length while room is held
            l.checkpoint("shop");
            {
                aof::log::reservation held(l, 1024);
                const uint64_t before = l.file_bytes();
                l.trim_to_last_checkpoint();
                check(l.records() == 0 && l.file_bytes() == before && l.free_bytes() >= 1024,
                      "a trim down to nothing keeps held room in the file");
            }
            l.checkpoint("shop");
            l.trim_to_last_checkpoint();
            check(l.file_bytes() == queue_file::initial_length,
                  "and gives the space back once nothing is held");
        }
        ::setrlimit(RLIMIT_FSIZE, &was);
        std::signal(SIGXFSZ, SIG_DFL);
    }

    ::unlink(path.c_str());
    std::printf("\n%s\n", failures == 0 ? "all aof log checks pass" : "FAILURES above");
    return failures == 0 ? 0 : 1;
}
