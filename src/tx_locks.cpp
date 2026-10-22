//
// Key locks for pessimistic transactions - TODO 719. See tx_locks.h.
//
#include "tx_locks.h"

#include <functional>
#include <stdexcept>

namespace barch {
    namespace {
        const std::string no_owner{};
        thread_local const std::string* writing_for = &no_owner;
        thread_local bool checking = false;
    }

    const std::string& tx_locks::owner_now() { return *writing_for; }
    bool tx_locks::guarding() { return checking; }

    tx_locks::owner_scope::owner_scope(const std::string& id) : outer(writing_for) { writing_for = &id; }
    tx_locks::owner_scope::~owner_scope() { writing_for = outer; }
    tx_locks::guard_scope::guard_scope(bool on) : outer(checking) { checking = on; }
    tx_locks::guard_scope::~guard_scope() { checking = outer; }

    std::string tx_locks::in_way(const entry& e, const std::string& owner, bool exclusive) {
        if (!e.exclusive.empty() && e.exclusive != owner)
            return e.exclusive;
        if (exclusive)
            for (const auto& s : e.shared)
                if (s != owner)
                    return s;
        return {};
    }

    std::vector<std::string> tx_locks::all_in_way(const entry& e, const std::string& owner, bool exclusive) {
        std::vector<std::string> out;
        if (!e.exclusive.empty() && e.exclusive != owner)
            out.push_back(e.exclusive);
        if (exclusive)
            for (const auto& s : e.shared)
                if (s != owner)
                    out.push_back(s);
        return out;
    }

    /*
     * Deadlock detection - TODO 724. A ring of waits can only close when a transaction
     * starts waiting, so looking each time one does, under `m`, finds every ring as it
     * forms: whoever started waiting just before is already in `waits`. From `start`,
     * follow each holder in its way to what that holder waits for, and so on. Coming back
     * to `start` is a ring. Only waiting transactions are followed, and each one once.
     */
    std::vector<std::string> tx_locks::ring_through(const std::string& start) const {
        std::set<std::string> seen;
        std::vector<std::string> path;
        std::function<bool(const std::string&)> follow = [&](const std::string& o) {
            const auto w = waits.find(o);
            if (w == waits.end())
                return false;
            const auto e = held.find(w->second.key);
            if (e == held.end())
                return false;
            for (const auto& b : all_in_way(e->second, o, w->second.exclusive)) {
                if (b == start)
                    return true;
                if (!seen.insert(b).second)
                    continue;
                path.push_back(b);
                if (follow(b))
                    return true;
                path.pop_back();
            }
            return false;
        };
        if (!follow(start))
            path.clear();
        return path;
    }

    tx_locks::taken tx_locks::take(const std::string& owner, const std::string& key, bool exclusive,
                                   std::chrono::steady_clock::time_point until) {
        std::unique_lock l(m);
        for (;;) {
            auto at = held.find(key);
            const std::string other = at == held.end() ? std::string() : in_way(at->second, owner, exclusive);
            if (other.empty()) {
                if (at == held.end()) {
                    at = held.emplace(key, entry{}).first;
                    count.store(held.size(), std::memory_order_release);
                }
                auto& e = at->second;
                if (exclusive) {
                    e.exclusive = owner;
                    e.shared.erase(owner);      // one lock, the stronger
                } else if (e.exclusive != owner) {
                    e.shared.insert(owner);
                }
                waits.erase(owner);
                return {true, {}};
            }
            waits[owner] = wait{key, exclusive};
            auto ring = ring_through(owner);
            if (!ring.empty()) {
                waits.erase(owner);
                auto first = ring.front();
                return {false, std::move(first), true, std::move(ring)};
            }
            if (freed.wait_until(l, until) == std::cv_status::timeout) {
                // one last look: it may have gone just as the time ran out
                at = held.find(key);
                const std::string still = at == held.end() ? std::string() : in_way(at->second, owner, exclusive);
                if (!still.empty())
                    return {false, still};
            }
        }
    }

    void tx_locks::release(const std::string& owner, const std::set<std::string>& keys) {
        {
            std::lock_guard l(m);
            waits.erase(owner);
            if (keys.empty())
                return;
            for (const auto& k : keys) {
                auto at = held.find(k);
                if (at == held.end())
                    continue;
                if (at->second.exclusive == owner)
                    at->second.exclusive.clear();
                at->second.shared.erase(owner);
                if (at->second.free())
                    held.erase(at);
            }
            count.store(held.size(), std::memory_order_release);
        }
        freed.notify_all();
    }

    void tx_locks::stop_waiting(const std::string& owner) {
        std::lock_guard l(m);
        waits.erase(owner);
    }

    std::string tx_locks::held_by_other(const std::string& owner, const std::string& key) const {
        if (!any())
            return {};
        std::lock_guard l(m);
        const auto at = held.find(key);
        return at == held.end() ? std::string() : in_way(at->second, owner, true);
    }

    std::string tx_locks::wait_free(const std::string& owner, const std::set<std::string>& keys,
                                    std::chrono::steady_clock::time_point until) {
        if (!any())
            return {};
        std::unique_lock l(m);
        for (;;) {
            std::string other;
            for (const auto& k : keys) {
                const auto at = held.find(k);
                if (at != held.end() && !(other = in_way(at->second, owner, true)).empty())
                    break;
            }
            if (other.empty())
                return {};
            if (freed.wait_until(l, until) == std::cv_status::timeout && std::chrono::steady_clock::now() >= until)
                return other;
        }
    }

    void tx_locks::check_write(const std::string& key) const {
        if (!checking || !any())
            return;
        const auto other = held_by_other(*writing_for, key);
        if (!other.empty())
            throw std::runtime_error("TXLOCKED the key is locked by transaction " + other +
                                     ", which holds it until it ends");
    }
}
