//
// One Raft group's durable state, in one file - TODO 610.
//
#include "group_store.h"
#include <chrono>
#include <thread>

#include "aof_record.h"
#include "lzr_log.h"
#include "replace_file.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

namespace barch::cluster {
    namespace {
        constexpr char magic[8] = {'b', 'r', 'a', 'f', 't', 'g', '0', '1'};
        // length, type and crc around a payload
        constexpr size_t record_overhead = 4 + 1 + 4;
        // index, term, value type and timestamp ahead of an entry's data
        constexpr size_t entry_head = 8 + 8 + 1 + 8;
        // a file this size or under is never worth rewriting
        constexpr uint64_t rewrite_floor = 4ull << 20;

        void put_u32(std::string& out, uint32_t v) {
            for (int i = 0; i < 4; ++i) out.push_back((char) ((v >> (8 * i)) & 0xff));
        }
        void put_u64(std::string& out, uint64_t v) {
            for (int i = 0; i < 8; ++i) out.push_back((char) ((v >> (8 * i)) & 0xff));
        }
        uint32_t get_u32(const uint8_t* p) {
            uint32_t v = 0;
            for (int i = 3; i >= 0; --i) v = (v << 8) | p[i];
            return v;
        }
        uint64_t get_u64(const uint8_t* p) {
            uint64_t v = 0;
            for (int i = 7; i >= 0; --i) v = (v << 8) | p[i];
            return v;
        }

        std::string buffer_bytes(const nuraft::buffer& b) {
            return {(const char*) b.data_begin(), b.size()};
        }
        nuraft::ptr<nuraft::buffer> to_buffer(const uint8_t* p, size_t n) {
            auto b = nuraft::buffer::alloc(n);
            if (n) std::memcpy(b->data_begin(), p, n);
            return b;
        }

        nuraft::ptr<nuraft::log_entry> clone(const nuraft::log_entry& e) {
            return nuraft::cs_new<nuraft::log_entry>(e.get_term(), nuraft::buffer::clone(e.get_buf()),
                                                     e.get_val_type(), e.get_timestamp(),
                                                     e.has_crc32(), e.get_crc32(), false);
        }

        // what last_entry and an index before the start answer with, as NuRaft's own
        // stores do: term 0 and an empty value
        nuraft::ptr<nuraft::log_entry> dummy() {
            return nuraft::cs_new<nuraft::log_entry>(0, nuraft::buffer::alloc(sizeof(uint64_t)));
        }

        uint64_t entry_bytes(const nuraft::log_entry& e) {
            return record_overhead + entry_head + e.get_buf().size();
        }

        // what arena::sync_dir_of does, without the arena code it lives beside
        void sync_dir(const std::string& path) {
            auto dir = std::filesystem::path(path).parent_path().string();
            if (dir.empty()) dir = ".";
            const int d = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            if (d < 0 || ::fsync(d) != 0)
                barch::err({"could not sync directory", dir, "-", std::strerror(errno)});
            if (d >= 0) ::close(d);
        }

        [[noreturn]] void die(const char* what) {
            barch::err({"stopping:", what});
            std::abort();
        }
    }

    group_store::group_store(std::string path) : file(std::move(path)) {
        open_file();
    }

    group_store::~group_store() {
        if (syncer.joinable()) {
            {
                std::lock_guard l(m);
                stopping = true;
            }
            sync_cv.notify_all();
            syncer.join();
        }
        if (fd >= 0) {
            if (unsynced) ::fdatasync(fd);
            ::close(fd);
        }
    }

    void group_store::open_file() {
        std::string content;
        {
            std::ifstream in(file, std::ios::binary);
            if (in) {
                std::ostringstream ss;
                ss << in.rdbuf();
                content = ss.str();
            }
        }
        fd = ::open(file.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd < 0)
            throw std::runtime_error("could not open the raft log " + file + ": " + std::strerror(errno));

        uint64_t good_end = 0;
        if (content.size() >= sizeof(magic) && std::memcmp(content.data(), magic, sizeof(magic)) == 0) {
            good_end = sizeof(magic);
            replay(content, good_end);
        } else if (!content.empty()) {
            ::close(fd);
            fd = -1;
            throw std::runtime_error("the raft log " + file + " isn't one this build writes");
        }

        if (content.empty()) {
            if (::write(fd, magic, sizeof(magic)) != (ssize_t) sizeof(magic))
                throw std::runtime_error("could not write the raft log " + file + ": " + std::strerror(errno));
            sync_or_die("starting the file");
            sync_dir(file);
            bytes = sizeof(magic);
        } else {
            if (good_end < content.size()) {
                // a write the process died in; everything before it is whole
                barch::warn({"the raft log", file, "ends in a torn record - cutting",
                             (uint64_t) (content.size() - good_end), "bytes off"});
                if (::ftruncate(fd, (off_t) good_end) != 0)
                    throw std::runtime_error("could not cut the raft log " + file + ": " + std::strerror(errno));
                sync_or_die("cutting a torn tail");
                cut_tail = true;
            }
            bytes = good_end;
        }
        if (::lseek(fd, (off_t) bytes, SEEK_SET) < 0)
            throw std::runtime_error("could not seek in the raft log " + file + ": " + std::strerror(errno));
        // what the file held when it was opened is on disk, as far as a restart
        // of this process goes
        durable = start + entries.size() - 1;
    }

    void group_store::replay(const std::string& content, uint64_t& good_end) {
        const auto* base = (const uint8_t*) content.data();
        uint64_t at = good_end;
        while (at + 4 <= content.size()) {
            const uint32_t n = get_u32(base + at);
            if (n < 1 || at + 4 + n + 4 > content.size())
                break;
            const uint8_t* body = base + at + 4;
            if (get_u32(body + n) != aof::crc32c(body, n))
                break;
            apply_record(body[0], body + 1, n - 1);
            at += 4 + n + 4;
            good_end = at;
        }
    }

    void group_store::apply_record(uint8_t type, const uint8_t* p, size_t n) {
        switch (type) {
            case rec_entry: {
                if (n < entry_head) return;
                const nuraft::ulong index = get_u64(p);
                const nuraft::ulong term = get_u64(p + 8);
                const auto vt = (nuraft::log_val_type) p[16];
                const uint64_t ts = get_u64(p + 17);
                auto e = nuraft::cs_new<nuraft::log_entry>(term, to_buffer(p + entry_head, n - entry_head),
                                                           vt, ts);
                if (entries.empty() && index != start)
                    start = index;      // the first entry after a compact past the end
                if (index < start)
                    return;
                const nuraft::ulong next = start + entries.size();
                if (index < next) {
                    // only written after a truncate, but cope if it wasn't
                    while (start + entries.size() > index) {
                        live_bytes -= entry_bytes(*entries.back());
                        entries.pop_back();
                    }
                } else if (index > next) {
                    return;             // a gap can't be served; nothing after it either
                }
                live_bytes += entry_bytes(*e);
                entries.push_back(std::move(e));
                return;
            }
            case rec_truncate: {
                if (n < 8) return;
                const nuraft::ulong from = get_u64(p);
                while (!entries.empty() && start + entries.size() > from) {
                    live_bytes -= entry_bytes(*entries.back());
                    entries.pop_back();
                }
                if (entries.empty() && from > start)
                    start = from;
                return;
            }
            case rec_compact: {
                if (n < 8) return;
                const nuraft::ulong upto = get_u64(p);
                while (!entries.empty() && start <= upto) {
                    live_bytes -= entry_bytes(*entries.front());
                    entries.pop_front();
                    ++start;
                }
                if (start <= upto)
                    start = upto + 1;
                return;
            }
            case rec_state:
                state_bytes = to_buffer(p, n);
                return;
            case rec_config:
                config_bytes = to_buffer(p, n);
                return;
            case rec_range:
                if (n < 16) return;
                range_from = get_u64(p);
                range_to = get_u64(p + 8);
                has_range = true;
                return;
            case rec_snapshot:
                snapshot_bytes = to_buffer(p, n);
                return;
            case rec_self:
                if (n < 4) return;
                self_id = (int32_t) get_u32(p);
                self_endpoint.assign((const char*) p + 4, n - 4);
                return;
            default:
                return;                 // a later build's record: skip it
        }
    }

    void group_store::write_record(uint8_t type, const std::string& payload) {
        std::string body;
        body.reserve(1 + payload.size());
        body.push_back((char) type);
        body.append(payload);
        std::string rec;
        rec.reserve(record_overhead + payload.size());
        put_u32(rec, (uint32_t) body.size());
        rec.append(body);
        put_u32(rec, aof::crc32c((const uint8_t*) body.data(), body.size()));
        size_t done = 0;
        while (done < rec.size()) {
            const ssize_t w = ::write(fd, rec.data() + done, rec.size() - done);
            if (w < 0) {
                if (errno == EINTR) continue;
                // the file may now hold part of this record, so nothing appended
                // after it could be trusted either
                barch::err({"could not write the raft log", file, "-", std::strerror(errno)});
                die("raft log write failed");
            }
            done += (size_t) w;
        }
        bytes += rec.size();
        unsynced = true;
    }

    void group_store::write_entry(nuraft::ulong index, const nuraft::log_entry& e) {
        std::string payload;
        payload.reserve(entry_head + e.get_buf().size());
        put_u64(payload, index);
        put_u64(payload, e.get_term());
        payload.push_back((char) e.get_val_type());
        put_u64(payload, e.get_timestamp());
        payload.append(buffer_bytes(e.get_buf()));
        write_record(rec_entry, payload);
    }

    void group_store::sync_or_die(const char* why) {
        (std::strcmp(why, "appending entries") == 0 ? entry_sync_count : other_sync_count)++;
        if (::fdatasync(fd) != 0) {
            barch::err({"could not sync the raft log", file, "while", why, "-", std::strerror(errno)});
            die("raft log sync failed");
        }
        unsynced = false;
        note_synced();
    }

    void group_store::note_synced() {
        durable = start + entries.size() - 1;
    }

    // ---- background syncing - TODO 625 ---------------------------------------------

    void group_store::sync_in_background(std::function<void()> notify) {
        std::lock_guard l(m);
        if (background)
            return;
        on_durable = std::move(notify);
        background = true;
        syncer = std::thread([this] { sync_loop(); });
    }

    nuraft::ulong group_store::last_durable_index() {
        std::lock_guard l(m);
        return background ? durable.load() : start + entries.size() - 1;
    }

    /*
     * One sync at a time, of everything written by the time it starts. Writes that
     * arrive while it runs wait for the next one, and share it. The sync is of a
     * duplicate of the descriptor, outside the lock, so appends go on meanwhile; a
     * rewrite that replaces the file moves the generation, and then this sync
     * claims nothing (the rewrite synced the new file itself).
     */
    void group_store::sync_loop() {
        std::unique_lock l(m);
        for (;;) {
            sync_cv.wait(l, [this] { return stopping || sync_wanted; });
            if (stopping)
                return;
            sync_wanted = false;
            if (!unsynced) {
                // a synchronous sync got there first, or there was nothing new
                l.unlock();
                if (on_durable) on_durable();
                l.lock();
                continue;
            }
            const uint64_t gen = generation;
            const nuraft::ulong target = start + entries.size() - 1;
            unsynced = false;           // anything written from here on wants the next sync
            const int d = ::dup(fd);
            l.unlock();
            ++entry_sync_count;
            if (const long ms = slow_ms.load(); ms > 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(ms));
            const bool ok = d >= 0 && ::fdatasync(d) == 0;
            const int saved = errno;
            if (d >= 0) ::close(d);
            if (!ok) {
                barch::err({"could not sync the raft log", file, "in the background -", std::strerror(saved)});
                die("raft log sync failed");
            }
            l.lock();
            if (generation == gen && durable.load() < target)
                durable = target;
            l.unlock();
            if (on_durable) on_durable();
            l.lock();
        }
    }

    nuraft::ptr<nuraft::log_entry> group_store::at(nuraft::ulong index) const {
        if (index < start || index >= start + entries.size())
            return nullptr;
        return entries[index - start];
    }

    nuraft::ulong group_store::next_slot() const {
        std::lock_guard l(m);
        return start + entries.size();
    }

    nuraft::ulong group_store::start_index() const {
        std::lock_guard l(m);
        return start;
    }

    nuraft::ptr<nuraft::log_entry> group_store::last_entry() const {
        std::lock_guard l(m);
        if (entries.empty())
            return dummy();
        return clone(*entries.back());
    }

    nuraft::ulong group_store::append(nuraft::ptr<nuraft::log_entry>& entry) {
        std::lock_guard l(m);
        const nuraft::ulong index = start + entries.size();
        auto e = clone(*entry);
        ++appended_count;
        write_entry(index, *e);
        live_bytes += entry_bytes(*e);
        entries.push_back(std::move(e));
        return index;
    }

    void group_store::write_at(nuraft::ulong index, nuraft::ptr<nuraft::log_entry>& entry) {
        std::lock_guard l(m);
        // entries from `index` on are being replaced, so whatever was durable there
        // isn't these
        ++generation;
        if (durable.load() >= index)
            durable = index - 1;
        std::string payload;
        put_u64(payload, index);
        write_record(rec_truncate, payload);
        while (!entries.empty() && start + entries.size() > index) {
            live_bytes -= entry_bytes(*entries.back());
            entries.pop_back();
        }
        if (entries.empty())
            start = index;
        auto e = clone(*entry);
        ++appended_count;
        write_entry(index, *e);
        live_bytes += entry_bytes(*e);
        entries.push_back(std::move(e));
    }

    void group_store::end_of_append_batch(nuraft::ulong, nuraft::ulong) {
        std::unique_lock l(m);
        if (background) {
            sync_wanted = true;
            l.unlock();
            sync_cv.notify_one();
            return;
        }
        if (unsynced)
            sync_or_die("appending entries");
    }

    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
    group_store::log_entries(nuraft::ulong from, nuraft::ulong to) {
        std::lock_guard l(m);
        auto out = nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();
        if (to <= from)
            return out;
        if (from < start || to > start + entries.size())
            return nullptr;             // compacted away, or not there yet
        out->reserve(to - from);
        for (nuraft::ulong i = from; i < to; ++i)
            out->push_back(clone(*entries[i - start]));
        return out;
    }

    nuraft::ptr<nuraft::log_entry> group_store::entry_at(nuraft::ulong index) {
        std::lock_guard l(m);
        auto e = at(index);
        return e ? clone(*e) : dummy();
    }

    nuraft::ulong group_store::term_at(nuraft::ulong index) {
        std::lock_guard l(m);
        auto e = at(index);
        return e ? e->get_term() : 0;
    }

    nuraft::ptr<nuraft::buffer> group_store::pack(nuraft::ulong index, nuraft::int32 cnt) {
        std::vector<nuraft::ptr<nuraft::buffer>> parts;
        size_t total = 0;
        {
            std::lock_guard l(m);
            for (nuraft::ulong i = index; i < index + (nuraft::ulong) cnt; ++i) {
                auto e = at(i);
                if (!e) break;
                auto b = e->serialize();
                total += b->size();
                parts.push_back(std::move(b));
            }
        }
        auto out = nuraft::buffer::alloc(sizeof(nuraft::int32) + parts.size() * sizeof(nuraft::int32) + total);
        out->pos(0);
        out->put((nuraft::int32) parts.size());
        for (auto& b : parts) {
            out->put((nuraft::int32) b->size());
            out->put(*b);
        }
        out->pos(0);
        return out;
    }

    void group_store::apply_pack(nuraft::ulong index, nuraft::buffer& packed) {
        packed.pos(0);
        const nuraft::int32 count = packed.get_int();
        std::lock_guard l(m);
        ++generation;
        // what NuRaft's own store does: these entries from `index` on, and the log
        // starts wherever they start if it held nothing before them
        std::string payload;
        put_u64(payload, index);
        write_record(rec_truncate, payload);
        while (!entries.empty() && start + entries.size() > index) {
            live_bytes -= entry_bytes(*entries.back());
            entries.pop_back();
        }
        if (entries.empty() || index > start + entries.size()) {
            if (!entries.empty()) {
                std::string upto;
                put_u64(upto, index - 1);
                write_record(rec_compact, upto);
                entries.clear();
                live_bytes = 0;
            }
            start = index;
        }
        for (nuraft::int32 i = 0; i < count; ++i) {
            const nuraft::int32 n = packed.get_int();
            auto b = nuraft::buffer::alloc((size_t) n);
            packed.get(b);
            auto e = nuraft::log_entry::deserialize(*b);
            ++appended_count;
            write_entry(start + entries.size(), *e);
            live_bytes += entry_bytes(*e);
            entries.push_back(std::move(e));
        }
        sync_or_die("applying a log pack");
    }

    static int64_t steady_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    void group_store::hold_for_snapshot(uint64_t snapshot_index) {
        // from the snapshot's own last entry: NuRaft needs its term to send the next
        snapshot_hold_from = snapshot_index;
        snapshot_hold_ms = steady_ms();
    }

    void group_store::compact_async(nuraft::ulong last_log_index,
                                    const nuraft::async_result<bool>::handler_type& when_done) {
        nuraft::ulong upto = last_log_index;
        uint64_t from = hold_from.load();
        // a snapshot sent within the last second: its member needs what comes after it
        if (const uint64_t s = snapshot_hold_from.load();
            s > 0 && steady_ms() - snapshot_hold_ms.load() < 1000 && (from == 0 || s < from))
            from = s;
        if (from > 0 && upto >= from) {
            upto = from - 1;
            const uint64_t cap = hold_cap.load();
            if (last_log_index > cap && upto < last_log_index - cap)
                upto = last_log_index - cap;
            if (upto < last_log_index)
                ++holds;
        }
        bool rc = compact(upto);
        nuraft::ptr<std::exception> none;
        when_done(rc, none);
    }

    bool group_store::compact(nuraft::ulong last_log_index) {
        std::lock_guard l(m);
        if (last_log_index < start)
            return true;
        std::string payload;
        put_u64(payload, last_log_index);
        write_record(rec_compact, payload);
        while (!entries.empty() && start <= last_log_index) {
            live_bytes -= entry_bytes(*entries.front());
            entries.pop_front();
            ++start;
        }
        if (start <= last_log_index)
            start = last_log_index + 1;
        sync_or_die("compacting");
        maybe_rewrite();
        return true;
    }

    bool group_store::flush() {
        std::lock_guard l(m);
        if (unsynced)
            sync_or_die("flushing");
        return true;
    }

    void group_store::save_state(const nuraft::srv_state& s) {
        auto b = s.serialize();
        std::lock_guard l(m);
        write_record(rec_state, buffer_bytes(*b));
        state_bytes = nuraft::buffer::clone(*b);
        // a vote that isn't durable can be cast twice after a restart
        sync_or_die("saving the term and vote");
    }

    nuraft::ptr<nuraft::srv_state> group_store::read_state() const {
        std::lock_guard l(m);
        if (!state_bytes)
            return nullptr;
        auto b = nuraft::buffer::clone(*state_bytes);
        return nuraft::srv_state::deserialize(*b);
    }

    void group_store::save_config(const nuraft::cluster_config& c) {
        auto b = c.serialize();
        std::lock_guard l(m);
        write_record(rec_config, buffer_bytes(*b));
        config_bytes = nuraft::buffer::clone(*b);
        sync_or_die("saving the configuration");
    }

    nuraft::ptr<nuraft::cluster_config> group_store::load_config() const {
        std::lock_guard l(m);
        if (!config_bytes)
            return nullptr;
        auto b = nuraft::buffer::clone(*config_bytes);
        return nuraft::cluster_config::deserialize(*b);
    }

    void group_store::save_self(int32_t server_id, const std::string& endpoint) {
        std::string payload;
        put_u32(payload, (uint32_t) server_id);
        payload.append(endpoint);
        std::lock_guard l(m);
        write_record(rec_self, payload);
        self_id = server_id;
        self_endpoint = endpoint;
        sync_or_die("saving who this node is");
    }

    void group_store::save_snapshot(nuraft::snapshot& snap) {
        auto b = snap.serialize();
        std::lock_guard l(m);
        write_record(rec_snapshot, buffer_bytes(*b));
        snapshot_bytes = nuraft::buffer::clone(*b);
        sync_or_die("saving a snapshot's index");
    }

    void group_store::save_range(uint64_t from, uint64_t to) {
        std::string payload;
        put_u64(payload, from);
        put_u64(payload, to);
        std::lock_guard l(m);
        write_record(rec_range, payload);
        range_from = from;
        range_to = to;
        has_range = true;
        sync_or_die("saving the group's shards");
    }

    bool group_store::range(uint64_t& from, uint64_t& to) const {
        std::lock_guard l(m);
        if (!has_range) return false;
        from = range_from;
        to = range_to;
        return true;
    }

    nuraft::ptr<nuraft::snapshot> group_store::last_snapshot() const {
        std::lock_guard l(m);
        if (!snapshot_bytes)
            return nullptr;
        auto b = nuraft::buffer::clone(*snapshot_bytes);
        return nuraft::snapshot::deserialize(*b);
    }

    bool group_store::self(int32_t& server_id, std::string& endpoint) const {
        std::lock_guard l(m);
        if (self_id < 0)
            return false;
        server_id = self_id;
        endpoint = self_endpoint;
        return true;
    }

    uint64_t group_store::file_bytes() const {
        std::lock_guard l(m);
        return bytes;
    }

    uint64_t group_store::rewrites() const {
        std::lock_guard l(m);
        return rewritten;
    }

    void group_store::maybe_rewrite() {
        if (bytes > rewrite_floor && bytes > 2 * (live_bytes + 4096))
            rewrite();
    }

    void group_store::rewrite() {
        ++generation;                   // a background sync of the old file claims nothing
        const std::string tmp = file + ".tmp";
        const int old = fd;
        const uint64_t old_bytes = bytes;
        fd = ::open(tmp.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            barch::warn({"could not rewrite the raft log", file, "-", std::strerror(errno), "- keeping it as it is"});
            fd = old;
            return;
        }
        bytes = 0;
        if (::write(fd, magic, sizeof(magic)) != (ssize_t) sizeof(magic)) {
            barch::warn({"could not rewrite the raft log", file, "-", std::strerror(errno), "- keeping it as it is"});
            ::close(fd);
            std::remove(tmp.c_str());
            fd = old;
            bytes = old_bytes;
            return;
        }
        bytes = sizeof(magic);
        if (self_id >= 0) {
            std::string me;
            put_u32(me, (uint32_t) self_id);
            me.append(self_endpoint);
            write_record(rec_self, me);
        }
        if (state_bytes) write_record(rec_state, buffer_bytes(*state_bytes));
        if (snapshot_bytes) write_record(rec_snapshot, buffer_bytes(*snapshot_bytes));
        if (has_range) {
            std::string r;
            put_u64(r, range_from);
            put_u64(r, range_to);
            write_record(rec_range, r);
        }
        if (config_bytes) write_record(rec_config, buffer_bytes(*config_bytes));
        if (start > 1) {
            std::string upto;
            put_u64(upto, start - 1);
            write_record(rec_compact, upto);
        }
        for (size_t i = 0; i < entries.size(); ++i)
            write_entry(start + i, *entries[i]);
        sync_or_die("rewriting");
        if (barch::replace_file(tmp.c_str(), file.c_str()) != 0) {
            barch::warn({"could not rename the rewritten raft log over", file, "-", std::strerror(errno)});
            ::close(fd);
            std::remove(tmp.c_str());
            fd = old;
            bytes = old_bytes;
            return;
        }
        sync_dir(file);
        ::close(old);
        ++rewritten;
    }
}
