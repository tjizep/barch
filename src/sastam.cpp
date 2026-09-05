//
// Created by linuxlite on 2/10/25.
//
#include "../external/include/valkeymodule.h"
#include "sastam.h"

#include <iostream>
#include <sys/types.h>
#include <sys/sysinfo.h>
#include "lzr_log.h"
#include <random>

static std::atomic<long long> physical_ram_cache{0};

long long get_total_physical_memory() {
    long long c = physical_ram_cache.load(std::memory_order_relaxed);
    if (!c) {
        struct sysinfo memInfo;
        sysinfo(&memInfo);
        c = (long long) memInfo.totalram * memInfo.mem_unit;
        physical_ram_cache.store(c, std::memory_order_relaxed);
    }
    return c;
}

enum {
    padding = 0,
    heap_checks = 1,
    use_malloc = 1
};

static size_t check_size = (heap_checks != 1) ? 0 : sizeof(uint32_t);
std::atomic<uint64_t> heap::allocated;
std::atomic<uint64_t> heap::vmm_allocated;

static uint32_t get_ptr_val(const void *v) {
    const auto *ptr = (const uint8_t *) v;
    auto ax = (uint64_t) ptr;
    ax &= ((1ll << 30) - 1);
    uint32_t ax32 = ax;
    return ax32;
}

uint64_t heap::random_range(uint64_t lower, uint64_t upper) {
    // one engine per thread: mt19937 is not safe to share, and hash resize
    // calls this from many shards at once. See TODO 214.
    static thread_local std::mt19937_64 gen{std::random_device{}()};
    std::uniform_int_distribution<uint64_t> dist(lower, upper);
    return dist(gen);
}

void *heap::allocate(size_t size) {
    if (!size) return nullptr;
    void *r = nullptr;
    if (use_malloc == 1) {
        r = malloc(size + padding + check_size);
    } else {
        r = ValkeyModule_Calloc(1, size + padding + check_size);
    }


    if (r) {
        memset(r, 0, size + padding + check_size);

        if (heap_checks) {
            uint32_t ax32 = get_ptr_val(r);
            memcpy((uint8_t *) r + size + padding, &ax32, sizeof(ax32));
            check_ptr(r, size);
        }
        auto actual = size;
        if (use_malloc != 1) {
            //ValkeyModule_MallocSize(r);
        }
        //if (size > 8 && actual > size*1.2)
        //    art::log({(size_t)allocated,"allocated:",actual,"vs:",size,"requested"});
        allocated += actual;
    }
    return r;
}

bool heap::valid_ptr(void *ptr, size_t size) {
    if (!ptr) return false;
    if (!size) return true;
    if (heap_checks != 1) return true;
    uint32_t ax32 = get_ptr_val(ptr);
    uint32_t ax32t = 0;
    memcpy(&ax32t, (const uint8_t *) ptr + size + padding, sizeof(ax32));
    return ax32t == ax32;
}

void heap::check_ptr(void *ptr, size_t size) {
    if (!ptr) return;
    if (!size) return;
    if (heap_checks != 1) return;
    uint32_t ax32 = get_ptr_val(ptr);
    uint32_t ax32t = 0;
    memcpy(&ax32t, (const uint8_t *) ptr + size + padding, sizeof(ax32));
    if (ax32t != ax32) {
        abort_with("memory check failed");
    }
}

void heap::free(void *ptr, size_t size) {
    if (ptr) {
        size_t actual = size;
        if (use_malloc != 1) {
            //actual = ValkeyModule_MallocSize(ptr);
        }
        check_ptr(ptr, size);
        if (use_malloc == 1) {
            ::free(ptr);
        } else {
            ValkeyModule_Free(ptr);
        }

        allocated -= actual;
        //if (size > 8 && actual > size*1.2)
        //    art::log({(size_t)allocated,"freed:",actual,"vs:",size,"requested"});
    }
}

void heap::free(void *ptr) {
    if (ptr) {
        size_t size = 0;
        if (use_malloc != 1) {
            ValkeyModule_Free(ptr);
        } else {
            free(ptr, size);
        }
    }
}

uint64_t heap::get_physical_memory_bytes() {
    return get_total_physical_memory();
}

double heap::get_physical_memory_ratio() {
    double r = use_malloc == 1 ? 0.0 : ValkeyModule_GetUsedMemoryRatio();
    if (r == 0.0f) {
        auto physical = (double) get_physical_memory_bytes();
        auto heap = (double) allocated;
        r = heap / physical;
    }
    return r;
}
#include <execinfo.h>
void abort_with(const char *message) __THROW {
    const size_t COUNT = 10;
    void* buffer[COUNT];
    int size = backtrace(buffer, COUNT);
    char** symbols = backtrace_symbols(buffer, size);
    barch::err({"There's a bug and we cannot continue - last reason [", message, "]"});
    if (symbols) {
        for (int s = 0; s < size; ++s) {
            barch::err({symbols[s]});
        }
    }
    abort();
}
