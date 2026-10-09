//
// What the rest of barch sees of clustering - TODO 610.
//
#include "cluster_hooks.h"

#include <algorithm>
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

    void bind(const std::string& canonical_space, binding_ptr b, size_t from, size_t to) {
        std::lock_guard l(bindings_lock());
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

    bool built() {
        return installed_runtime.load() != nullptr;
    }

    bool command(const std::vector<std::string>& args, std::vector<std::string>& out, std::string& err) {
        auto* r = installed_runtime.load();
        if (!r) {
            err = "ERR this build has no cluster - configure it with -DBARCH_CLUSTER=ON";
            return false;
        }
        return r->command(args, out, err);
    }
}
