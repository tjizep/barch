//
// One Raft group's durable state, in one file - TODO 610.
//
#ifndef BARCH_CLUSTER_GROUP_STORE_H
#define BARCH_CLUSTER_GROUP_STORE_H

#include "nuraft.hxx"

#include <cstdint>
#include <deque>
#include <mutex>
#include <string>

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
     *   - snapshot: NuRaft's serialized snapshot - the index and term the data
     *               files are known to hold at least, and the configuration then
     *               (TODO 611)
     *
     * Opening the file replays every record. A record that's short or fails its
     * crc ends the file there: it's the tail of a write the process died in, and
     * the file is cut back to the last whole record.
     *
     * NuRaft counts an entry as durable once end_of_append_batch returns, so
     * that's where the file is synced. A failed sync stops the process: after one
     * the kernel may have dropped pages it couldn't write, and a later sync can
     * succeed without them. Raft can't promise anything about a log that may have
     * lost writes it already acknowledged.
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
        bool flush() override;

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
        /** false when this file was never given one */
        bool self(int32_t& server_id, std::string& endpoint) const;

        [[nodiscard]] const std::string& path() const { return file; }
        /** bytes in the file now - for tests and INFO */
        [[nodiscard]] uint64_t file_bytes() const;
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
            rec_snapshot = 7
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
        int32_t self_id{-1};
        std::string self_endpoint{};
        uint64_t bytes{0};          // the file's length
        uint64_t live_bytes{0};     // roughly what a rewrite would write
        uint64_t rewritten{0};
        bool cut_tail{false};
        bool unsynced{false};
    };
}

#endif //BARCH_CLUSTER_GROUP_STORE_H
