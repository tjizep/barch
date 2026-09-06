#include "staged.h"

#include "function_api.h"
#include "lzr_log.h"
#include "shard.h"
#include "sharded_store.h"

namespace barch {

staged::staged(const key_space_ptr& space) : space(space) {
}

void staged::add(kind what, const std::string& name, const std::string& value) {
    const bool fn = what == kind::fn_set || what == kind::fn_remove;
    std::string tag = (fn ? "f" : "k") + name;
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
        s.name = o.name;
        if (s.fn)
            s.had = barch::functions::source_in(space, o.name, s.value);
        else
            s.had = acc.get(o.name, s.value) == foreign::store_access::read_state::present;
        before.push_back(std::move(s));
    }

    const bool one_shard = space->get_shard_count() == 1;
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
     */
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
                case kind::fn_set:     ok = barch::functions::install(space, o->name, o->value, e); break;
                case kind::fn_remove:  barch::functions::remove(space, o->name); break;
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
            put_back();
            return false;
        }
        if (again.empty())
            break;
        if (progress == 0) {
            // nothing moved this pass, so nothing will next pass either
            err = last;
            put_back();
            return false;
        }
        left.swap(again);
    }

    if (one_shard)
        store.each_shard([](const barch::shard_ptr& t) { t->commit(); });
    return true;
}

}
