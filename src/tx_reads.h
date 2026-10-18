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
#include "value_type.h"

#include <cstdint>

namespace barch {
    class abstract_shard;

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
