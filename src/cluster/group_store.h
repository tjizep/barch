//
// One Raft group's durable state, in one file - TODO 610.
//
#ifndef BARCH_CLUSTER_GROUP_STORE_H
#define BARCH_CLUSTER_GROUP_STORE_H

#include "nuraft.hxx"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

namespace barch::cluster {
    /*
     * NuRaft's log store and the state it keeps beside the log (the current term,
     * the vote, the cluster configuration), all in one append only file. Two
     * files that have to agree can stop agreeing after a crash, so it's one.
     *
     * The file is an 8 byte magic and then records:
     *
     *     u32 length   of what follows, up to the crc
     *     u8  type     entry, truncate, compact, state or config
     *     ...          the payload
     *     u32 crc32c   over type and payload
     *
     * little endian, written a byte at a time like the change log's records.
     *
     *   - entry:    u64 index, u64 term, u8 value type, u64 timestamp, the data
     *   - truncate: u64 index - entries from here on are gone
     *   - compact:  u64 index - entries up to and including this are gone
     *   - state:    NuRaft's serialized srv_state
     *   - config:   NuRaft's serialized cluster_config
     *   - self:     u32 server id, then this node's endpoint - who this node is in
     *               the group, which NuRaft asks for before anything else
     *   - range:    u64 from, u64 to - the run of its space's shards the group
     *               owns, once a split has changed it (TODO 616)
     *   - snapshot: NuRaft's serialized snapshot - the index and term the data
     *               files are known to hold at least, and the configuration then
     *               (TODO 611)
     *
     * Opening the file replays every record. A record that's short or fails its
     * crc ends the file there: it's the tail of a write the process died in, and
     * the file is cut back to the last whole record.
     *
     * Without background syncing, NuRaft counts an entry as durable once
     * end_of_append_batch returns, so that's where the file is synced. A failed
     * sync stops the process, in the background too: after one
     * the kernel may have dropped pages it couldn't write, and a later sync can
     * succeed without them. Raft can't promise anything about a log that may have
     * lost writes it already acknowledged.
     *
     * With background syncing on (TODO 625), end_of_append_batch only asks for a
     * sync, and a thread of the store's does it. last_durable_index says how far
     * the file is synced, and `on_durable` is called each time that moves, which
     * is what NuRaft's parallel log appending needs. A leader then commits on its
     * followers' durable copies while its own sync is still going, and the writes
     * that arrive meanwhile share the next sync, rather than taking one each.
     * Everything else that syncs still does it before returning.
     *
     * When compaction leaves more dead records than live ones the file is
     * rewritten: the live state into `<file>.tmp`, synced, renamed over the file,
     * and the directory synced.
     */
    class group_store : public nuraft::log_store {
    public:
        explicit group_store(std::string path);
        ~group_store() override;

        group_store(const group_store&) = delete;
        group_store& operator=(const group_store&) = delete;

        // ---- nuraft::log_store ----
        nuraft::ulong next_slot() const override;
        nuraft::ulong start_index() const override;
        nuraft::ptr<nuraft::log_entry> last_entry() const override;
        nuraft::ulong append(nuraft::ptr<nuraft::log_entry>& entry) override;
        void write_at(nuraft::ulong index, nuraft::ptr<nuraft::log_entry>& entry) override;
        void end_of_append_batch(nuraft::ulong start, nuraft::ulong cnt) override;
        nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
            log_entries(nuraft::ulong start, nuraft::ulong end) override;
        nuraft::ptr<nuraft::log_entry> entry_at(nuraft::ulong index) override;
        nuraft::ulong term_at(nuraft::ulong index) override;
        nuraft::ptr<nuraft::buffer> pack(nuraft::ulong index, nuraft::int32 cnt) override;
        void apply_pack(nuraft::ulong index, nuraft::buffer& pack) override;
        bool compact(nuraft::ulong last_log_index) override;
        /**
         * The compaction NuRaft asks for after each snapshot, held back by
         * hold_log - TODO 636. A follower installing a snapshot calls compact()
         * instead, which isn't held: its log has to go up to the snapshot.
         */
        void compact_async(nuraft::ulong last_log_index,
                           const nuraft::async_result<bool>::handler_type& when_done) override;
        /**
         * Keep entries from `from` on, which a member still needs, but no more than
         * `cap` behind what a compaction asks for. 0 for `from` holds nothing.
         */
        void hold_log(uint64_t from, uint64_t cap) {
            hold_cap = cap;
            hold_from = from;
        }
        /**
         * The leader is sending a snapshot at `snapshot_index`: keep the entries after
         * it for as long as it keeps being sent, and a second after. Takes effect at
         * once, unlike hold_log, which the cluster's tick sets - TODO 636.
         */
        void hold_for_snapshot(uint64_t snapshot_index);
        /** how many compactions kept more than they were asked to - for tests and INFO */
        [[nodiscard]] uint64_t log_holds() const { return holds.load(); }
        bool flush() override;
        nuraft::ulong last_durable_index() override;

        /**
         * Sync appended entries on a thread of the store's from now on, and call
         * `on_durable` (from that thread, holding no lock of the store's) each time
         * more of the log is durable - TODO 625.
         */
        void sync_in_background(std::function<void()> on_durable);

        // ---- what the state manager keeps here ----
        void save_state(const nuraft::srv_state& state);
        nuraft::ptr<nuraft::srv_state> read_state() const;
        void save_config(const nuraft::cluster_config& config);
        nuraft::ptr<nuraft::cluster_config> load_config() const;
        void save_self(int32_t server_id, const std::string& endpoint);
        /** the last snapshot the data files hold; synced before it returns - TODO 611 */
        void save_snapshot(nuraft::snapshot& s);
        /** null when there's never been one */
        nuraft::ptr<nuraft::snapshot> last_snapshot() const;
        /** the shards the group owns; synced before it returns - TODO 616 */
        void save_range(uint64_t from, uint64_t to);
        /** false when no range was ever saved */
        bool range(uint64_t& from, uint64_t& to) const;
        /** false when this file was never given one */
        bool self(int32_t& server_id, std::string& endpoint) const;

        [[nodiscard]] const std::string& path() const { return file; }
        /** bytes in the file now - for tests and INFO */
        [[nodiscard]] uint64_t file_bytes() const;
        /**
         * since the file was opened - TODO 630: entries appended, syncs made for
         * them (in the background or at the end of a batch), and every other sync
         * (term and vote, configuration, compaction, snapshots, rewrites)
         */
        struct sync_stats {
            uint64_t appended{0};
            uint64_t entry_syncs{0};
            uint64_t other_syncs{0};
        };
        [[nodiscard]] sync_stats syncs() const {
            return {appended_count.load(), entry_sync_count.load(), other_sync_count.load()};
        }
        /** how many times the file has been rewritten since it was opened - for tests */
        [[nodiscard]] uint64_t rewrites() const;
        /** whether opening it cut a torn tail off - for tests */
        [[nodiscard]] bool tail_was_cut() const { return cut_tail; }

    private:
        enum : uint8_t {
            rec_entry = 1,
            rec_truncate = 2,
            rec_compact = 3,
            rec_state = 4,
            rec_config = 5,
            rec_self = 6,
            rec_snapshot = 7,
            rec_range = 8
        };

        void open_file();
        void replay(const std::string& bytes, uint64_t& good_end);
        void apply_record(uint8_t type, const uint8_t* p, size_t n);
        void write_record(uint8_t type, const std::string& payload);
        void write_entry(nuraft::ulong index, const nuraft::log_entry& e);
        void sync_or_die(const char* why);
        void maybe_rewrite();
        void rewrite();
        nuraft::ptr<nuraft::log_entry> at(nuraft::ulong index) const;

        std::string file;
        int fd{-1};
        mutable std::mutex m;
        nuraft::ulong start{1};
        std::deque<nuraft::ptr<nuraft::log_entry>> entries{};
        nuraft::ptr<nuraft::buffer> state_bytes{};
        nuraft::ptr<nuraft::buffer> config_bytes{};
        nuraft::ptr<nuraft::buffer> snapshot_bytes{};
        bool has_range{false};
        uint64_t range_from{0};
        uint64_t range_to{0};
        int32_t self_id{-1};
        std::string self_endpoint{};
        uint64_t bytes{0};          // the file's length
        uint64_t live_bytes{0};     // roughly what a rewrite would write
        uint64_t rewritten{0};
        bool cut_tail{false};
        bool unsynced{false};
        std::atomic<uint64_t> appended_count{0};
        std::atomic<uint64_t> entry_sync_count{0};
        std::atomic<uint64_t> other_sync_count{0};
        std::atomic<uint64_t> hold_from{0};
        std::atomic<uint64_t> hold_cap{0};
        std::atomic<uint64_t> holds{0};
        std::atomic<uint64_t> snapshot_hold_from{0};
        std::atomic<int64_t> snapshot_hold_ms{0};     // steady clock, when last sent

        // ---- background syncing - TODO 625 ----
        void sync_loop();
        void note_synced();                     // under m: all of it is durable
        // the last index known to be in the file and synced
        std::atomic<nuraft::ulong> durable{0};
        // moved by anything that replaces entries or the file, so a sync that
        // began before it doesn't claim what it never covered
        uint64_t generation{0};
        bool background{false};
        bool sync_wanted{false};
        bool stopping{false};
        std::condition_variable sync_cv;
        std::function<void()> on_durable{};
        std::thread syncer{};
    };
}

#endif //BARCH_CLUSTER_GROUP_STORE_H
