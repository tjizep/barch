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

    std::vector<std::string> tx_locks::writers_waiting(const std::string& key, const std::string& owner) const {
        std::vector<std::string> out;
        const auto at = held.find(key);
        if (at != held.end() && (at->second.exclusive == owner || at->second.shared.count(owner) > 0))
            return out;
        for (const auto& [o, w] : waits)
            if (o != owner && w.exclusive && w.key == key)
                out.push_back(o);
        return out;
    }

    bool tx_locks::parked_writer(const std::string& other, const std::string& key) const {
        const auto w = waits.find(other);
        return w != waits.end() && w->second.exclusive && w->second.parked && w->second.key == key;
    }

    std::string tx_locks::blocker(const std::string& key, const std::string& owner, bool exclusive) const {
        const auto at = held.find(key);
        if (at != held.end()) {
            auto other = in_way(at->second, owner, exclusive);
            if (!other.empty())
                return other;
        }
        if (!exclusive) {
            auto ahead = writers_waiting(key, owner);
            if (!ahead.empty())
                return ahead.front();
        }
        if (exclusive)
            for (const auto& [o, list] : spans)
                if (o != owner)
                    for (const auto& s : list)
                        if (s.covers(key)) {
                            span_blocked.insert(key);
                            return o;
                        }
        return {};
    }

    std::pair<std::string, std::string> tx_locks::span_blocker(const span& s, const std::string& owner) const {
        for (auto at = held.lower_bound(s.lo); at != held.end() && s.covers(at->first); ++at)
            if (!at->second.exclusive.empty() && at->second.exclusive != owner)
                return {at->first, at->second.exclusive};
        return {};
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
            std::vector<std::string> in_the_way;
            const auto e = held.find(w->second.key);
            if (e != held.end())
                in_the_way = all_in_way(e->second, o, w->second.exclusive);
            // a key's reader waits behind its waiting writers - TODO 869
            if (!w->second.exclusive && !w->second.span)
                for (auto& b : writers_waiting(w->second.key, o))
                    in_the_way.push_back(std::move(b));
            // a span over the key is in the way of a write to it - TODO 749
            if (w->second.exclusive)
                for (const auto& [so, list] : spans)
                    if (so != o)
                        for (const auto& sp : list)
                            if (sp.covers(w->second.key)) {
                                in_the_way.push_back(so);
                                break;
                            }
            for (const auto& b : in_the_way) {
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
        // `until` already past: only tried, as a parked session does - TODO 721
        const bool parks = until <= std::chrono::steady_clock::now();
        for (;;) {
            const std::string other = blocker(key, owner, exclusive);
            if (other.empty()) {
                auto at = held.find(key);
                if (at == held.end()) {
                    at = held.emplace(key, entry{}).first;
                    counted();
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
            waits[owner] = wait{key, exclusive, false, parks};
            auto ring = ring_through(owner);
            if (!ring.empty()) {
                if (unwait(owner))
                    freed.notify_all();
                auto first = ring.front();
                return {false, std::move(first), key, true, std::move(ring)};
            }
            if (freed.wait_until(l, until) == std::cv_status::timeout) {
                // one last look: it may have gone just as the time ran out
                const std::string still = blocker(key, owner, exclusive);
                if (!still.empty()) {
                    taken out{false, still, key};
                    out.behind_parked = parked_writer(still, key);
                    return out;
                }
            }
        }
    }

    tx_locks::taken tx_locks::take_span(const std::string& owner, const span& s,
                                        std::chrono::steady_clock::time_point until) {
        std::unique_lock l(m);
        for (;;) {
            auto [key, other] = span_blocker(s, owner);
            if (other.empty()) {
                auto& mine = spans[owner];
                bool had = false;
                for (const auto& h : mine)
                    if (h.covers(s)) {
                        had = true;
                        break;
                    }
                if (!had) {
                    mine.push_back(s);
                    ++span_count;
                    counted();
                }
                waits.erase(owner);
                return {true, {}};
            }
            // waiting for the one key in the way, as a shared lock on it would
            waits[owner] = wait{key, false, true};
            auto ring = ring_through(owner);
            if (!ring.empty()) {
                waits.erase(owner);
                auto first = ring.front();
                return {false, std::move(first), std::move(key), true, std::move(ring)};
            }
            if (freed.wait_until(l, until) == std::cv_status::timeout) {
                auto still = span_blocker(s, owner);
                if (!still.second.empty())
                    return {false, std::move(still.second), std::move(still.first)};
            }
        }
    }

    std::set<std::string> tx_locks::release(const std::string& owner, const std::set<std::string>& keys) {
        std::set<std::string> wake = keys;
        {
            std::lock_guard l(m);
            unwait(owner);
            const auto had = spans.find(owner);
            if (keys.empty() && had == spans.end() && writer_left.empty())
                return wake;
            wake.insert(writer_left.begin(), writer_left.end());
            writer_left.clear();
            if (had != spans.end()) {
                span_count -= had->second.size();
                spans.erase(had);
                // all of them, not only the ones in this owner's spans: one still held
                // by another span parks again, and says so again
                wake.insert(span_blocked.begin(), span_blocked.end());
                span_blocked.clear();
            }
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
            counted();
        }
        freed.notify_all();
        return wake;
    }

    bool tx_locks::unwait(const std::string& owner) {
        const auto w = waits.find(owner);
        if (w == waits.end())
            return false;
        const bool writer = w->second.exclusive;
        if (writer)
            writer_left.insert(w->second.key);
        waits.erase(w);
        return writer;
    }

    std::set<std::string> tx_locks::stop_waiting(const std::string& owner) {
        std::set<std::string> wake;
        {
            std::lock_guard l(m);
            if (!unwait(owner))
                return wake;
            wake.swap(writer_left);
        }
        freed.notify_all();
        return wake;
    }

    bool tx_locks::waiting(const std::string& owner) const {
        std::lock_guard l(m);
        return waits.count(owner) > 0;
    }

    std::string tx_locks::held_by_other(const std::string& owner, const std::string& key) const {
        if (!any())
            return {};
        std::lock_guard l(m);
        return blocker(key, owner, true);
    }

    std::string tx_locks::wait_free(const std::string& owner, const std::set<std::string>& keys,
                                    std::chrono::steady_clock::time_point until) {
        if (!any())
            return {};
        std::unique_lock l(m);
        for (;;) {
            std::string other;
            for (const auto& k : keys)
                if (!(other = blocker(k, owner, true)).empty())
                    break;
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
