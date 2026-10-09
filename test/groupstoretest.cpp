// A Raft group's durable state in one file - TODO 610. Everything the log store
// says before a reopen has to still be true after one.
#include "cluster/group_store.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>

using barch::cluster::group_store;
using namespace nuraft;

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-58s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

static ptr<log_entry> entry(ulong term, const std::string& data,
                            log_val_type t = log_val_type::app_log) {
    auto b = buffer::alloc(data.size());
    std::memcpy(b->data_begin(), data.data(), data.size());
    return cs_new<log_entry>(term, b, t);
}

static std::string data_of(const ptr<log_entry>& e) {
    if (!e) return "<null>";
    return {(const char*) e->get_buf().data_begin(), e->get_buf().size()};
}

static void append(group_store& s, ulong term, const std::string& data) {
    auto e = entry(term, data);
    const ulong at = s.append(e);
    s.end_of_append_batch(at, 1);
}

// entries [from, to) hold "v<i>"
static bool holds(group_store& s, ulong from, ulong to) {
    if (s.start_index() != from || s.next_slot() != to) return false;
    for (ulong i = from; i < to; ++i)
        if (data_of(s.entry_at(i)) != "v" + std::to_string(i)) return false;
    return true;
}

int main() {
    namespace fs = std::filesystem;
    const fs::path dir = fs::current_path() / "groupstoretest.d";
    fs::remove_all(dir);
    fs::create_directories(dir);
    const std::string file = (dir / "group.raft").string();

    std::printf("an empty log\n");
    {
        group_store s(file);
        check(s.start_index() == 1 && s.next_slot() == 1, "starts at 1 with nothing in it");
        check(s.last_entry()->get_term() == 0, "its last entry is the term 0 dummy");
        check(!s.read_state() && !s.load_config(), "no state and no config yet");
    }

    std::printf("appends survive a reopen\n");
    {
        group_store s(file);
        for (ulong i = 1; i <= 10; ++i) append(s, 1 + i / 4, "v" + std::to_string(i));
        check(holds(s, 1, 11), "ten entries in");
    }
    {
        group_store s(file);
        check(holds(s, 1, 11), "and the same ten after a reopen");
        check(s.term_at(3) == 1 && s.term_at(4) == 2 && s.term_at(10) == 3, "with their terms");
        check(s.last_entry()->get_term() == 3, "last_entry is the tenth");
        check(s.log_entries(2, 5)->size() == 3, "a range reads back");
        check(s.log_entries(5, 20) == nullptr, "a range past the end is null");
    }

    std::printf("write_at cuts the log\n");
    {
        group_store s(file);
        auto e = entry(4, "v6");
        s.write_at(6, e);
        s.end_of_append_batch(6, 1);
        check(holds(s, 1, 7) && s.term_at(6) == 4, "six entries, the sixth replaced");
    }
    {
        group_store s(file);
        check(holds(s, 1, 7) && s.term_at(6) == 4, "the same after a reopen");
    }

    std::printf("compaction\n");
    {
        group_store s(file);
        s.compact(3);
        check(holds(s, 4, 7), "compact(3) leaves 4 to 6");
        check(s.log_entries(2, 5) == nullptr, "a range that starts before them is null");
        check(s.term_at(2) == 0, "and a compacted term is 0");
    }
    {
        group_store s(file);
        check(holds(s, 4, 7), "the same after a reopen");
        s.compact(100);
        check(s.start_index() == 101 && s.next_slot() == 101, "compact past the end moves the start");
        append(s, 5, "v101");
        check(holds(s, 101, 102), "and the next append lands there");
    }
    {
        group_store s(file);
        check(holds(s, 101, 102), "the same after a reopen");
    }

    std::printf("state and configuration\n");
    {
        group_store s(file);
        srv_state st(7, 2, true, false);
        s.save_state(st);
        cluster_config c(12, 11);
        c.get_servers().push_back(cs_new<srv_config>(1, "127.0.0.1:1"));
        c.get_servers().push_back(cs_new<srv_config>(2, "127.0.0.1:2"));
        s.save_config(c);
        s.save_self(2, "127.0.0.1:2");
    }
    {
        group_store s(file);
        auto st = s.read_state();
        check(st && st->get_term() == 7 && st->get_voted_for() == 2, "the term and vote come back");
        auto c = s.load_config();
        check(c && c->get_log_idx() == 12 && c->get_servers().size() == 2, "the configuration comes back");
        int32_t id = -1;
        std::string ep;
        check(s.self(id, ep) && id == 2 && ep == "127.0.0.1:2", "and who this node is");
        check(holds(s, 101, 102), "beside the log");
    }

    std::printf("a torn tail\n");
    {
        group_store s(file);
        append(s, 5, "v102");
        append(s, 5, "v103");
    }
    {
        const auto size = fs::file_size(file);
        fs::resize_file(file, size - 3);
        group_store s(file);
        check(s.tail_was_cut(), "is noticed");
        check(holds(s, 101, 103), "and only the torn entry is lost");
        append(s, 5, "v103");
    }
    {
        group_store s(file);
        check(!s.tail_was_cut() && holds(s, 101, 104), "and the file is whole again after it");
        check(s.read_state() && s.read_state()->get_term() == 7, "with the state still there");
    }

    std::printf("pack and apply_pack\n");
    {
        group_store a(file);
        const std::string other = (dir / "other.raft").string();
        {
            group_store b(other);
            for (ulong i = 1; i <= 3; ++i) append(b, 1, "x" + std::to_string(i));
            auto p = a.pack(101, 3);
            b.apply_pack(101, *p);
            check(holds(b, 101, 104), "a pack past the end replaces the log");
        }
        {
            group_store b(other);
            check(holds(b, 101, 104), "and stays that way after a reopen");
            auto p = a.pack(102, 2);
            b.apply_pack(102, *p);
            check(holds(b, 101, 104), "a pack inside it replaces the tail");
        }
    }

    std::printf("a file mostly compacted away is rewritten\n");
    {
        const std::string big = (dir / "big.raft").string();
        uint64_t before = 0;
        {
            group_store s(big);
            const std::string pad(64 * 1024, 'p');
            for (ulong i = 1; i <= 100; ++i) {
                auto e = entry(1, "v" + std::to_string(i) + pad);
                s.append(e);
            }
            s.end_of_append_batch(1, 100);
            srv_state st(3, 1, true, false);
            s.save_state(st);
            s.save_self(1, "127.0.0.1:9");
            check(!s.last_snapshot(), "no snapshot before one is saved");
            uint64_t rf = 0, rt = 0;
            check(!s.range(rf, rt), "no range before one is saved");
            s.save_range(6, 12);
            snapshot snap(95, 1, cs_new<cluster_config>(90, 80), 0, snapshot::logical_object);
            s.save_snapshot(snap);
            before = s.file_bytes();
            s.compact(95);
            check(s.rewrites() == 1, "once");
            check(s.file_bytes() < before / 4, "and it's much smaller");
            check(s.start_index() == 96 && s.next_slot() == 101, "with the same entries");
        }
        group_store s(big);
        check(s.start_index() == 96 && s.next_slot() == 101, "the same after a reopen");
        check(data_of(s.entry_at(100)).rfind("v100", 0) == 0, "with the data");
        check(s.read_state() && s.read_state()->get_term() == 3, "and the state");
        int32_t id = -1;
        std::string ep;
        check(s.self(id, ep) && id == 1 && ep == "127.0.0.1:9", "and who this node is");
        auto snap = s.last_snapshot();
        check(snap && snap->get_last_log_idx() == 95 && snap->get_last_log_term() == 1,
              "and the snapshot, through a rewrite");
        uint64_t rf = 0, rt = 0;
        check(s.range(rf, rt) && rf == 6 && rt == 12, "and the group's range");
        check(!fs::exists(big + ".tmp"), "and no temporary left");
    }

    std::printf("syncing in the background\n");
    {
        // TODO 625: end_of_append_batch only asks for a sync, the store's own
        // thread does it, and last_durable_index says how far it has got
        const std::string bg = (dir / "background.raft").string();
        group_store s(bg);
        std::atomic<int> told{0};
        s.sync_in_background([&told] { ++told; });
        auto settle = [&](ulong want) {
            for (int i = 0; i < 500 && s.last_durable_index() < want; ++i)
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            return s.last_durable_index() == want;
        };
        check(s.last_durable_index() == 0, "an empty log has nothing durable");
        for (int i = 1; i <= 3; ++i) {
            auto e = entry(1, "v" + std::to_string(i));
            s.append(e);
        }
        check(s.last_durable_index() == 0, "appended entries aren't durable before the batch ends");
        s.end_of_append_batch(1, 3);
        check(settle(3) && told > 0, "after it, all three are, and the store says so");
        auto e4 = entry(1, "v4");
        s.append(e4);
        s.end_of_append_batch(4, 1);
        check(settle(4), "and so is the next one");
        // a follower's log is overwritten from 3 on: what was durable there isn't these
        auto r = entry(2, "w3");
        s.write_at(3, r);
        check(s.last_durable_index() == 2, "writing over entry 3 takes durable back to 2 (" +
                                               std::to_string(s.last_durable_index()) + ")");
        s.end_of_append_batch(3, 1);
        check(settle(3) && data_of(s.entry_at(3)) == "w3", "until it's synced too");
        s.flush();
        check(s.last_durable_index() == 3, "a flush leaves all of it durable");
    }
    {
        group_store again((dir / "background.raft").string());
        check(again.next_slot() == 4 && data_of(again.entry_at(3)) == "w3" &&
              again.last_durable_index() == 3, "and opened again, the file holds the overwrite");
    }

    fs::remove_all(dir);
    std::printf("%s\n", failures ? "FAILED" : "all passed");
    return failures ? 1 : 0;
}
