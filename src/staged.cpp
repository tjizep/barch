#include "staged.h"

#include <algorithm>

#include "cluster_hooks.h"

#include "function_api.h"
#include "meta_keys.h"
#include "lzr_log.h"
#include "shard.h"
#include "sharded_store.h"

namespace barch {

staged::staged(const key_space_ptr& space) : space(space) {
}

void staged::add(kind what, const std::string& name, const std::string& value) {
    const bool fn = what == kind::fn_set || what == kind::fn_remove;
    const bool meta = what == kind::meta_set || what == kind::meta_remove;
    std::string tag = (fn ? "f" : meta ? "m" : "k") + name;
    auto found = at.find(tag);
    if (found != at.end()) {
        // the first position, the last value: a caller that stages a key twice meant
        // the second value, but the ordering it chose the first time still stands
        ops[found->second].what = what;
        ops[found->second].value = value;
        return;
    }
    at[tag] = ops.size();
    op o;
    o.what = what;
    o.name = name;
    o.value = value;
    ops.push_back(std::move(o));
}

void staged::set(const std::string& key, const std::string& value) {
    add(kind::key_set, key, value);
}

void staged::remove(const std::string& key) {
    add(kind::key_remove, key, {});
}

void staged::set_function(const std::string& name, const std::string& source) {
    add(kind::fn_set, name, source);
}

void staged::remove_function(const std::string& name) {
    add(kind::fn_remove, name, {});
}

void staged::set_meta(const std::string& name, const std::string& value) {
    add(kind::meta_set, name, value);
}

void staged::remove_meta(const std::string& name) {
    add(kind::meta_remove, name, {});
}

void staged::abort() {
    ops.clear();
    at.clear();
}

bool staged::commit(std::string& err) {
    err.clear();
    if (ops.empty())
        return true;
    if (!space) {
        err = "no key space";
        return false;
    }
    auto acc = barch::functions::store_for_owner(space);
    if (!acc.set || !acc.get || !acc.remove) {
        err = "this key space cannot be written";
        return false;
    }

    // what every target holds now, so a failure part way can put it back
    heap::vector<snapshot> before;
    before.reserve(ops.size());
    for (const auto& o : ops) {
        snapshot s;
        s.fn = o.what == kind::fn_set || o.what == kind::fn_remove;
        s.meta = o.what == kind::meta_set || o.what == kind::meta_remove;
        s.name = o.name;
        if (s.fn)
            s.had = barch::functions::source_in(space, o.name, s.value);
        else if (s.meta)
            s.had = barch::meta::get(space, o.name, s.value);
        else
            s.had = acc.get(o.name, s.value) == foreign::store_access::read_state::present;
        before.push_back(std::move(s));
    }

    // a shard's own rollback undoes writes on this node only, so a space replicated
    // with Raft puts each value back with a write instead - TODO 663
    const bool one_shard = space->get_shard_count() == 1 && !space->has_raft();
    barch::sharded_store store(space);
    if (one_shard)
        store.each_shard([](const barch::shard_ptr& t) { t->begin(); });

    auto put_back = [&]() {
        if (one_shard) {
            store.each_shard([](const barch::shard_ptr& t) { t->rollback(); });
            return;
        }
        for (const auto& s : before) {
            std::string e;
            bool ok;
            if (s.fn)
                ok = s.had ? barch::functions::install(space, s.name, s.value, e)
                           : barch::functions::remove(space, s.name);
            else if (s.meta)
                ok = s.had ? barch::meta::set(space, s.name, s.value, e)
                           : barch::meta::remove(space, s.name);
            else
                ok = s.had ? acc.set(s.name, s.value, e) : acc.remove(s.name);
            // best effort, and said out loud: a rollback that cannot write has left
            // the space in a state nobody asked for and silence would be worse
            if (!ok && s.had)
                barch::err({"staged rollback could not restore", s.name, e});
        }
    };

    /*
     * Applied in order, except that a stored function which will not install is set
     * aside and tried again after the rest, for as long as anything is making
     * progress. That is how a module gets installed before the function that
     * requires it without the caller having to work out the order - it is the apply
     * that discovers it. A key that fails is a failure straight away.
     *
     * `sources`, in a replicated space, is where the functions installed so far
     * are, since the store won't have them until the batch commits.
     */
    auto apply_all = [&](barch::functions::gathered_sources* sources) -> bool {
        heap::vector<const op*> left;
        left.reserve(ops.size());
        for (const auto& o : ops)
            left.push_back(&o);

        std::string last;
        while (!left.empty()) {
            heap::vector<const op*> again;
            size_t progress = 0;
            for (const auto* o : left) {
                std::string e;
                bool ok = true;
                switch (o->what) {
                    case kind::key_set:    ok = acc.set(o->name, o->value, e); break;
                    case kind::key_remove: acc.remove(o->name); break;
                    case kind::fn_set:
                        ok = barch::functions::install(space, o->name, o->value, e);
                        if (ok && sources) sources->set(space, o->name, o->value);
                        break;
                    case kind::fn_remove:
                        barch::functions::remove(space, o->name);
                        if (sources) sources->remove(space, o->name);
                        break;
                    case kind::meta_set:   ok = barch::meta::set(space, o->name, o->value, e); break;
                    case kind::meta_remove: barch::meta::remove(space, o->name); break;
                }
                if (ok) {
                    ++progress;
                    continue;
                }
                last = (e.empty() ? "could not write " + o->name : o->name + ": " + e);
                if (o->what == kind::fn_set) {
                    again.push_back(o);
                    continue;
                }
                err = last;
                return false;
            }
            if (again.empty())
                break;
            if (progress == 0) {
                // nothing moved this pass, so nothing will next pass either
                err = last;
                return false;
            }
            left.swap(again);
        }
        return true;
    };

    if (space->has_raft())
        return commit_replicated(apply_all, put_back, err);

    if (!apply_all(nullptr)) {
        put_back();
        return false;
    }
    if (one_shard)
        store.each_shard([](const barch::shard_ptr& t) { t->commit(); });
    return true;
}

/*
 * In a space replicated with Raft - TODO 665. Each write is gathered rather than
 * made: the shard builds its record and takes the write back out. Then the lot goes
 * to the space's group as one entry, which every member applies whole as it
 * commits, this one too. Refused or failed part way, nothing was written anywhere,
 * so there's nothing to put back.
 *
 * A space split over several groups gets one entry per group, in turn. Each is
 * whole, but a later one refused leaves the earlier ones in, so they're put back
 * with writes of their own, which go through Raft like any other.
 */
bool staged::commit_replicated(const std::function<bool(barch::functions::gathered_sources*)>& apply_all,
                               const std::function<void()>& put_back, std::string& err) {
    std::vector<barch::cluster::batched> got;
    {
        barch::cluster::batching gathering(got);
        barch::functions::gathered_sources sources;
        if (!apply_all(&sources))
            return false;
    }
    size_t committed = 0;
    if (!barch::cluster::commit_gathered(got, err, &committed)) {
        if (committed > 0)
            put_back();
        return false;
    }
    return true;
}

}
