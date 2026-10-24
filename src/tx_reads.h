#pragma once
//
// What a transaction reads through its overlay - TODO 701. See docs/TRANSACTIONS.md.
//
// A transaction's commands run against its overlay, a space that DEPENDS on the one
// the transaction is in. A key the overlay doesn't hold is read from the space
// beneath, and that's a read the transaction's COMMITTX has to check is still so.
// The shard tells whatever log is open on its thread, at the one place it reads
// through (shard::through_source); the log keeps only its own overlay's reads.
//
// The overlay keeps a copy of every key it reads, so the log doesn't keep the keys
// again - TODO 755. It counts them, and keeps the read's hash only for a key that's
// then written, from the leaf just before the write. Any other leaf in the overlay is
// still what was read.
//
#include "value_type.h"
#include "sastam.h"

#include <memory>

#include <cstdint>
#include <string>

namespace barch {
    class abstract_shard;

    /** what a key was, from a before-image - TODO 701 */
    struct tx_seen {
        bool present = false;
        std::string value{};
        int64_t expiry = 0;
        bool is_volatile = false;
        bool hashed = false;
    };

    class tx_read_log {
    public:
        virtual ~tx_read_log() = default;
        /**
         * `shard` read `key` from beneath it: whether it's there, and if so its plain
         * value (never the compressed form, which is each node's own) and its deadline.
         * True when it's the overlay's first read of the key: the shard keeps what it
         * read (the leaf, or a tomb for nothing), so a second read gives the same.
         */
        virtual bool saw(const abstract_shard* shard, art::value_type key, bool present, art::value_type value,
                         int64_t expiry) = 0;
        /** whether `shard` is one of this log's overlay, whose reads and writes it tracks - TODO 755 */
        [[nodiscard]] virtual bool overlays(const abstract_shard* shard) const {
            (void) shard;
            return false;
        }
        /**
         * `shard`, one of the overlay's, is about to change `key`, and this is what its
         * leaf there holds now: the copy of the first read, or an earlier write - TODO
         * 755. The first call for a key keeps it, since the leaf is about to stop saying
         * what was read.
         */
        virtual void writing(const abstract_shard* shard, art::value_type key, bool present, art::value_type value,
                             int64_t expiry) {
            (void) shard; (void) key; (void) present; (void) value; (void) expiry;
        }
        /**
         * `key` in `source` as this transaction's snapshot saw it, when a write since has
         * saved what it was (a before-image) - TODO 701. False when none has, and the
         * source answers as it is.
         */
        virtual bool as_of(const abstract_shard* source, art::value_type key, tx_seen& out) {
            (void) source; (void) key; (void) out;
            return false;
        }
        /**
         * For a walk inside the transaction - TODO 701: the snapshot's layers to put
         * under `top`, when it's a shard of this transaction's overlay, oldest first.
         */
        virtual void layers_under(const abstract_shard* top, heap::vector<std::shared_ptr<abstract_shard>>& out) {
            (void) top; (void) out;
        }
        /**
         * A walk of [lo, hi) of `space` (as stored keys; an empty hi has no end) inside
         * the transaction, which COMMITTX checks nothing has been written into since its
         * snapshot: a phantom - TODO 701.
         */
        virtual void saw_range(const void* space, art::value_type lo, art::value_type hi) {
            (void) space; (void) lo; (void) hi;
        }
    };

    /** the log open on this thread, or null */
    tx_read_log* tx_reading();

    /** opens `log` on this thread for as long as it lives */
    class tx_reading_scope {
    public:
        explicit tx_reading_scope(tx_read_log* log);
        ~tx_reading_scope();
        tx_reading_scope(const tx_reading_scope&) = delete;
        tx_reading_scope& operator=(const tx_reading_scope&) = delete;
    private:
        tx_read_log* outer;
    };
}
