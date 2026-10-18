//
// What the rest of barch sees of clustering - TODO 610.
//
#include "cluster_hooks.h"
#include "abstract_shard.h"
#include "key_space.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <thread>
#include <random>
#include <cstdlib>
#include <atomic>
#include <map>
#include <mutex>
#include <stdexcept>

namespace barch::cluster {
    namespace {
        std::mutex& bindings_lock() {
            static auto* m = new std::mutex;
            return *m;
        }
        // never destroyed: shards hold on to what they were given, and may
        // outlive the statics at exit - TODO 57
        std::map<std::string, std::vector<shard_binding>>& bindings() {
            static auto* b = new std::map<std::string, std::vector<shard_binding>>;
            return *b;
        }
        std::atomic<runtime*> installed_runtime{nullptr};
    }

    namespace {
        std::map<std::string, uint64_t>& committed_here() {
            thread_local std::map<std::string, uint64_t> c;
            return c;
        }
    }

    namespace {
        struct read_policy {
            session* s{nullptr};
            std::map<const binding*, binding_ptr> read{};
        };
        read_policy& reads_here() {
            thread_local read_policy p;
            return p;
        }
    }

    bool readable(binding& b, session& s) {
        return b.leader_read() || (s.follower_reads && b.follower_read(s.after[b.label()], 200));
    }

    void begin_reads(session* s) {
        auto& p = reads_here();
        p.s = s;
        p.read.clear();
    }

    void check_read(const binding_ptr& b, size_t shard) {
        auto& p = reads_here();
        if (!p.s || !b)
            return;
        if (shard != SIZE_MAX) {
            std::string why;
            if (b->fenced(shard, why))
                throw std::runtime_error(why);
        }
        if (p.read.count(b.get()))
            return;
        if (!readable(*b, *p.s))
            throw std::runtime_error(b->not_leader());
        p.read[b.get()] = b;
    }

    void end_reads() {
        auto& p = reads_here();
        if (p.s) {
            for (const auto& [raw, b] : p.read) {
                auto& seen = p.s->after[b->label()];
                if (const auto at = b->applied(); at > seen) seen = at;
            }
        }
        p.s = nullptr;
        p.read.clear();
    }

    void note_committed(const std::string& space, uint64_t index) {
        auto& c = committed_here();
        auto& at = c[space];
        if (index > at) at = index;
    }

    void clear_committed() {
        committed_here().clear();
    }

    const std::map<std::string, uint64_t>& committed() {
        return committed_here();
    }

    std::vector<shard_binding> bindings_for(const std::string& canonical_space) {
        std::lock_guard l(bindings_lock());
        auto i = bindings().find(canonical_space);
        return i == bindings().end() ? std::vector<shard_binding>{} : i->second;
    }

    namespace {
        // how many spaces are bound, read without the lock - TODO 663
        std::atomic<size_t> bound_spaces{0};
    }

    bool any_bound() {
        return bound_spaces.load(std::memory_order_acquire) != 0;
    }

    void bind(const std::string& canonical_space, binding_ptr b, size_t from, size_t to) {
        std::lock_guard l(bindings_lock());
        struct count_after {
            ~count_after() { bound_spaces.store(bindings().size(), std::memory_order_release); }
        } count;
        auto& v = bindings()[canonical_space];
        for (auto& sb : v) {
            if (sb.from == from && sb.to == to) {
                sb.b = std::move(b);
                return;
            }
        }
        v.push_back({from, to, std::move(b)});
    }

    void unbind(const std::string& canonical_space, const binding_ptr& b) {
        std::lock_guard l(bindings_lock());
        struct count_after {
            ~count_after() { bound_spaces.store(bindings().size(), std::memory_order_release); }
        } count;
        auto i = bindings().find(canonical_space);
        if (i == bindings().end()) return;
        if (!b) {
            bindings().erase(i);
            return;
        }
        auto& v = i->second;
        v.erase(std::remove_if(v.begin(), v.end(), [&](const shard_binding& sb) { return sb.b == b; }), v.end());
        if (v.empty()) bindings().erase(i);
    }

    namespace {
        std::mutex shard_counts_lock;
        std::map<std::string, size_t>& shard_counts() {
            static std::map<std::string, size_t> m;
            return m;
        }
    }

    namespace {
        thread_local const void* narrowing_shard = nullptr;
    }
    void set_narrowing(const void* shard) { narrowing_shard = shard; }
    const void* narrowing() { return narrowing_shard; }

    namespace {
        thread_local std::vector<batched>* gathering = nullptr;
    }
    batching::batching(std::vector<batched>& into) : outer(gathering) { gathering = &into; }
    batching::~batching() { gathering = outer; }
    std::vector<batched>* batch_now() { return gathering; }

    namespace {
        thread_local std::string diverged;
    }
    void mark_diverged(const std::string& why) { diverged = why; }
    std::string take_diverged() {
        std::string out;
        out.swap(diverged);
        return out;
    }

    namespace {
        /** a random id for a batch across groups */
        std::string batch_id() {
            static std::atomic<uint64_t> seq{0};
            std::random_device rd;
            char buf[48];
            std::snprintf(buf, sizeof(buf), "%08x%08x-%llx", rd(), rd(), (unsigned long long) ++seq);
            return buf;
        }

        /** [at, end) of `got` as one batch across its groups - TODO 666 */
        bool commit_across(std::vector<batched>& got, size_t at, size_t end, std::string& why) {
            std::vector<binding_ptr> groups;
            for (size_t i = at; i < end; ++i)
                if (std::find(groups.begin(), groups.end(), got[i].via) == groups.end())
                    groups.push_back(got[i].via);
            if (groups.size() == 1 && groups[0]->leader()) {
                std::vector<std::string> records;
                std::vector<size_t> shards;
                for (size_t i = at; i < end; ++i) {
                    records.push_back(std::move(got[i].record));
                    shards.push_back(got[i].shard);
                }
                return groups[0]->commit_batch(records, shards, why) == binding::result::committed;
            }
            std::string list;
            for (const auto& g : groups)
                list += (list.empty() ? "" : ",") + std::to_string(g->group_number());
            std::vector<std::string> words{batch_id(), list};
            for (size_t i = at; i < end; ++i)
                words.push_back(std::move(got[i].record));
            // test knob: a pause after the first group has it, for a test to kill the writer in
            static const long pause_ms = [] {
                const char* v = std::getenv("BARCH_TEST_XBATCH_PAUSE_MS");
                return v ? std::atol(v) : 0L;
            }();
            for (size_t k = 0; k < groups.size(); ++k) {
                std::vector<size_t> mine;
                for (size_t i = at; i < end; ++i)
                    if (got[i].via == groups[k])
                        mine.push_back(got[i].shard);
                std::string w;
                const auto r = groups[k]->commit_xbatch(words, mine, w);
                if (r != binding::result::committed) {
                    if (k == 0) {
                        why = w;            // in no group: as if never sent
                        return false;
                    }
                    // decided: the leaders roll it forward into the rest
                    why = "UNKNOWN the batch is in, and goes on to the rest of its groups: " + w;
                    return false;
                }
                if (k == 0 && pause_ms > 0)
                    std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms));
            }
            // every group has it; a close that doesn't land is made by the next leader
            for (const auto& g : groups) {
                std::string w;
                g->commit_xclose(words[0], w);
            }
            return true;
        }
    }

    bool commit_gathered(std::vector<batched>& got, std::string& why, size_t* committed) {
        struct part {
            binding_ptr via;
            std::vector<std::string> records;
            std::vector<size_t> shards;
        };
        /*
         * One entry per group, cut where it would pass `batch_bytes` - TODO 681: a
         * LOADFS of a big directory was one entry of any size. Each entry still lands
         * whole; a batch over the bound lands in pieces, in order, and one refused
         * partway leaves the pieces before it in (`committed` counts them).
         */
        static const size_t batch_bytes = [] {
            const char* v = std::getenv("BARCH_TEST_BATCH_BYTES");     // test knob
            const long long n = v ? std::atoll(v) : 0;
            return n > 0 ? (size_t) n : (size_t) 16 * 1024 * 1024;
        }();
        /*
         * A batch over several groups - TODO 666. It used to be one entry per group in
         * turn, so a writer that died between two left the first groups' parts in. Now
         * every group's log gets the whole batch as one entry (each member applies its
         * group's records), in turn, and once the first has it the batch is decided:
         * if the writer goes before the rest have it, a leader that finds a group
         * without it sends it there (cluster.cpp, recover_xbatches). Cut at the same
         * bound, each piece a batch of its own.
         */
        {
            std::vector<binding_ptr> groups;
            for (const auto& b : got)
                if (std::find(groups.begin(), groups.end(), b.via) == groups.end())
                    groups.push_back(b.via);
            /*
             * And a batch for one group this node doesn't lead goes through that
             * group's leader the same way - TODO 689: a split space's function runs where
             * it was sent, and its writes may all be another group's.
             */
            if (groups.size() > 1 || (groups.size() == 1 && !groups[0]->leader())) {
                if (committed)
                    *committed = 0;         // nothing to put back: the batch goes on, or never started
                size_t at = 0;
                while (at < got.size()) {
                    size_t end = at, bytes = 0;
                    while (end < got.size() && (end == at || bytes + got[end].record.size() <= batch_bytes))
                        bytes += got[end++].record.size();
                    if (!commit_across(got, at, end, why))
                        return false;
                    at = end;
                }
                return true;
            }
        }
        std::vector<part> parts;
        std::vector<size_t> part_bytes;
        for (auto& b : got) {
            // the latest entry for this group, unless adding this one would pass the bound
            ptrdiff_t at = -1;
            for (ptrdiff_t i = (ptrdiff_t) parts.size() - 1; i >= 0; --i)
                if (parts[(size_t) i].via == b.via) {
                    at = i;
                    break;
                }
            if (at < 0 || (!parts[(size_t) at].records.empty()
                           && part_bytes[(size_t) at] + b.record.size() > batch_bytes)) {
                parts.push_back({b.via, {}, {}});
                part_bytes.push_back(0);
                at = (ptrdiff_t) parts.size() - 1;
            }
            part_bytes[(size_t) at] += b.record.size();
            parts[(size_t) at].records.push_back(std::move(b.record));
            parts[(size_t) at].shards.push_back(b.shard);
        }
        for (size_t i = 0; i < parts.size(); ++i) {
            if (committed)
                *committed = i;
            if (parts[i].via->commit_batch(parts[i].records, parts[i].shards, why) != binding::result::committed)
                return false;
        }
        if (committed)
            *committed = parts.size();
        return true;
    }

    namespace {
        std::mutex runner_lock;
        command_runner& runner() {
            static command_runner r;
            return r;
        }
    }
    void set_command_runner(command_runner r) {
        std::lock_guard l(runner_lock);
        runner() = std::move(r);
    }
    bool run_command(const std::string& space, const std::vector<std::string>& argv, uint64_t index,
                     reply_ptr& reply, std::string& err) {
        command_runner r;
        {
            std::lock_guard l(runner_lock);
            r = runner();
        }
        if (!r) {
            err = "this node can't run commands from the log";
            return false;
        }
        return r(space, argv, index, reply, err);
    }

    void set_space_shards(const std::string& space, size_t shards) {
        std::lock_guard l(shard_counts_lock);
        shard_counts()[space] = shards;
    }

    size_t space_shards(const std::string& space) {
        std::lock_guard l(shard_counts_lock);
        const auto i = shard_counts().find(space);
        return i == shard_counts().end() ? 0 : i->second;
    }

    void install(runtime* r) {
        installed_runtime.store(r);
    }

    bool start(std::string& err) {
        auto* r = installed_runtime.load();
        return r ? r->start(err) : true;
    }

    void stop() {
        if (auto* r = installed_runtime.load())
            r->stop();
    }

    void quiesce() {
        if (auto* r = installed_runtime.load())
            r->quiesce();
    }

    bool built() {
        return installed_runtime.load() != nullptr;
    }

    bool push_tx(uint32_t home, uint32_t at, const std::string& holder, const std::string& mine, std::string& why) {
        auto* r = installed_runtime.load();
        if (!r) {
            why = "this build has no cluster";
            return false;
        }
        return r->push_tx(home, at, holder, mine, why);
    }

    bool command(const std::vector<std::string>& args, std::vector<std::string>& out, std::string& err) {
        auto* r = installed_runtime.load();
        if (!r) {
            err = "ERR this build has no cluster - configure it with -DBARCH_CLUSTER=ON";
            return false;
        }
        return r->command(args, out, err);
    }

    bool log_server_command(const std::shared_ptr<barch::key_space>& space, const std::vector<std::string>& words,
                            reply_ptr& reply, std::string& why, binding_ptr via) {
        auto groups = space->raft_groups();
        if (via)
            groups.insert(groups.begin(), via);
        if (groups.empty()) {
            why = "TRYAGAIN this space's raft group isn't bound here yet";
            return false;
        }
        std::vector<std::string> argv{"", ""};
        argv.insert(argv.end(), words.begin(), words.end());
        auto committed = binding::result::refused;
        const auto shards = space->get_shards();
        size_t held = 0;
        try {
            for (; held < shards.size(); ++held)
                shards[held]->command_begin();
            committed = groups[0]->commit_command(space->get_canonical_name(), argv, reply, why);
        } catch (const std::exception& e) {
            why = e.what();
        }
        for (size_t i = 0; i < held; ++i)
            shards[i]->command_end();
        if (committed != binding::result::committed) {
            if (why.empty())
                why = "ERR the entry wasn't committed";
            return false;
        }
        return true;
    }

    namespace {
        thread_local bool shards_set = false;
        thread_local size_t shards_from = 0, shards_to = 0;
    }

    run_shards::run_shards(size_t from, size_t to)
        : was_from(shards_from), was_to(shards_to), was_set(shards_set) {
        shards_from = from;
        shards_to = to;
        shards_set = true;
    }

    run_shards::~run_shards() {
        shards_from = was_from;
        shards_to = was_to;
        shards_set = was_set;
    }

    bool running_shards(size_t& from, size_t& to) {
        if (!shards_set)
            return false;
        from = shards_from;
        to = shards_to;
        return true;
    }

    bool run_takes_shard(size_t shard) {
        return !shards_set || (shard >= shards_from && shard < shards_to);
    }

    run_range current_run() {
        run_range r;
        r.set = shards_set;
        r.from = shards_from;
        r.to = shards_to;
        return r;
    }
}
