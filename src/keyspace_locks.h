//
// Created by teejip on 10/30/25.
//

#ifndef BARCH_KEYSPACE_LOCKS_H
#define BARCH_KEYSPACE_LOCKS_H
#include "key_space.h"
extern void lock_shared(barch::key_space_ptr ks) ;
extern void lock_unique(barch::key_space_ptr ks) ;
extern void unlock(barch::key_space_ptr ks) ;
extern void unlock_shared(barch::key_space_ptr ks);
struct ks_shared {
    ordered_lock<read_lock> locks;
    ks_shared() = delete;
    explicit ks_shared(barch::key_space_ptr s) : locks(s) {
    }

    ~ks_shared() = default;
};
struct ks_unique {
    ordered_lock<storage_release> locks;
    explicit ks_unique(barch::key_space_ptr s) : locks(s) {
    }
    ~ks_unique()  = default;
};
#endif //BARCH_KEYSPACE_LOCKS_H