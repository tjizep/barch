#pragma once
//
// staged - a set of writes that lands whole, or not at all. See TODO 255.
//
// The pattern was in three places before this: `apply_dest` in function_sync.cpp,
// and both directory importers in fs_api.cpp, each with its own snapshot vector and
// its own roll_back lambda, and only one of them using the single shard fast path.
//
// WHAT IT PROMISES, and what it does not:
//
//   * Atomicity, not isolation. A space with several shards has no single latch to
//     take, so a reader running concurrently can see some shards applied and others
//     not. What is guaranteed is that a *failure* leaves nothing behind. The one
//     exception is a space with a single shard, where the shard's own
//     begin/commit/rollback is used and the whole apply really is a transaction -
//     which the configuration space gets for nothing, being one shard by
//     construction.
//
//   * Rollback is best effort. Putting a value back can fail the same way writing
//     it did. It is logged and the commit still reports the original failure,
//     because that is the one worth acting on.
//
#include <string>

#include "key_space.h"
#include "sastam.h"

namespace barch {

class staged {
public:
    explicit staged(const key_space_ptr& space);

    /**
     * Writes are applied in the order they are added, which is how a caller
     * expresses an ordering the store has to see - the file layout writes chunks
     * before the metadata that points at them, and `staged` does not need to know
     * why. Adding the same target twice keeps the first position and the last
     * value, so a caller may overwrite what it staged.
     */
    void set(const std::string& key, const std::string& value);
    void remove(const std::string& key);
    /** a stored function, through the same install path SETF uses */
    void set_function(const std::string& name, const std::string& source);
    void remove_function(const std::string& name);

    size_t size() const { return ops.size(); }
    bool empty() const { return ops.empty(); }

    /**
     * Apply the lot. Empty `err` is success; anything else is the first failure and
     * nothing was left behind.
     *
     * A stored function that will not install because a module it requires is not
     * there yet is retried once the rest have been applied, for as long as any
     * progress is being made. That ordering is discovered by the apply rather than
     * known by the caller, which is why it lives here. Plain keys are not retried:
     * a key that will not write will not write.
     */
    bool commit(std::string& err);
    void abort();

private:
    enum class kind { key_set, key_remove, fn_set, fn_remove };
    struct op {
        kind what{kind::key_set};
        std::string name;
        std::string value;
    };
    struct snapshot {
        bool fn{false};
        std::string name;
        std::string value;
        bool had{false};
    };
    void add(kind what, const std::string& name, const std::string& value);

    key_space_ptr space;
    heap::vector<op> ops;
    heap::string_map<size_t> at;      // "k<name>" / "f<name>" -> index in ops
};

}
