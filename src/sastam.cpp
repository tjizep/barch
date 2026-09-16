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
#include <algorithm>
#include <cstring>
#include <fstream>
#include <chrono>
#include <vector>
#include <mutex>
#include <unistd.h>
#include "configuration.h"

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
std::atomic<uint64_t> heap::named_vmm_allocated;
std::atomic<uint64_t> heap::luau_allocated;

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

void *heap::luau_reallocate(void *ptr, size_t osize, size_t nsize) {
    if (nsize == 0) {
        heap::free(ptr, osize);
        luau_allocated -= osize;
        return nullptr;
    }
    void *r = heap::allocate(nsize);
    if (!r)
        return nullptr;                     // out of memory, and Luau will say so
    if (ptr != nullptr && osize > 0) {
        memcpy(r, ptr, std::min(osize, nsize));
        heap::free(ptr, osize);
    }
    if (nsize > osize) {
        luau_allocated += nsize - osize;
    } else {
        luau_allocated -= osize - nsize;
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


uint64_t heap::working_set_bytes() {
    const uint64_t all = allocated.load(std::memory_order_relaxed);
    const uint64_t named = named_vmm_allocated.load(std::memory_order_relaxed);
    // the subtraction is guarded rather than trusted: the two counters are
    // maintained by different paths, and a limit computed from an underflow
    // would be enormous and useless rather than merely wrong
    return all > named ? all - named : all;
}

/** this process's cgroup v2 path, from /proc/self/cgroup, or empty */
static std::string own_cgroup_path() {
    std::ifstream f("/proc/self/cgroup");
    std::string line;
    while (std::getline(f, line)) {
        // the v2 line is the one with an empty controller list: "0::/path"
        if (line.rfind("0::", 0) == 0) {
            return line.substr(3);
        }
    }
    return {};
}

/** the process ids in a cgroup's cgroup.procs, empty if it cannot be read */
static std::vector<long> cgroup_members(const std::string& dir) {
    std::vector<long> pids;
    std::ifstream f(dir + "/cgroup.procs");
    long pid = 0;
    while (f >> pid) {
        pids.push_back(pid);
    }
    return pids;
}

/*
 * The `memory.max` this process set, if it set one - TODO 349. Remembered so
 * that turning the control off puts back only a limit of our own making, and by
 * path rather than by a flag, so a change of `cgroup_memory_path` still releases
 * the file that was actually written.
 */
static std::mutex written_mut;
static std::string written_file;

/** say a refusal once, not once per maintenance tick */
static void say_once(const std::string& why) {
    static std::mutex mut;
    static std::string said;
    std::lock_guard lock(mut);
    if (said == why)
        return;
    said = why;
    barch::err({"cgroup memory.max not set:", why});
}

bool heap::apply_cgroup_memory_max(std::string& why) {
    if (!barch::get_cgroup_memory_control()) {
        why = "cgroup_memory_control is off";
        return false;
    }
    /*
     * Once every few seconds at most. Every key space's maintenance thread calls
     * this and they all compute the same number, so without a gate the limit
     * would be rewritten once per space per tick.
     */
    static std::atomic<int64_t> last_ns{0};
    const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    auto last = last_ns.load(std::memory_order_relaxed);
    if (last != 0 && now_ns - last < 2'000'000'000ll) {
        why = "written recently";
        return false;
    }
    if (!last_ns.compare_exchange_strong(last, now_ns, std::memory_order_relaxed)) {
        why = "another thread is writing it";
        return false;
    }

    /*
     * Which cgroup, and whether it is ours to bound - TODO 348.
     *
     * `memory.max` binds a cgroup, not a process, so writing it to a cgroup this
     * process merely landed in sets a limit on everything else in there. A
     * derived path therefore has to be ours alone, which is what a delegated
     * cgroup of our own looks like. Naming one in `cgroup_memory_path` is how
     * somebody says they know what is in it - membership is still required,
     * because bounding a cgroup we are not even in is never right.
     */
    std::string dir = barch::get_cgroup_memory_path();
    const bool derived = dir.empty();
    if (derived) {
        const auto path = own_cgroup_path();
        if (path.empty()) {
            why = "no cgroup v2 line in /proc/self/cgroup";
            say_once(why);
            return false;
        }
        dir = "/sys/fs/cgroup" + path;
    }

    const auto members = cgroup_members(dir);
    if (members.empty()) {
        why = "cannot read " + dir + "/cgroup.procs";
        say_once(why);
        return false;
    }
    const long me = ::getpid();
    if (std::find(members.begin(), members.end(), me) == members.end()) {
        why = "this process is not in " + dir;
        say_once(why);
        return false;
    }
    if (derived && members.size() > 1) {
        why = dir + " holds " + std::to_string(members.size()) + " processes, not just"
              " this one - a limit there would bind them too. Give barchd a cgroup of"
              " its own (systemd Delegate=yes) or name one in cgroup_memory_path";
        say_once(why);
        return false;
    }

    const std::string file = dir + "/memory.max";

    const uint64_t working = working_set_bytes();
    const uint64_t target = working + barch::get_cgroup_memory_headroom();
    /*
     * A floor, because the failure mode is asymmetric. Too high a limit does
     * nothing; too low a one cannot be met by reclaiming page cache, so the
     * kernel kills the process instead. 64MB is below anything barch starts up
     * in, so a target under it means the counters are wrong rather than the
     * process being small.
     */
    static constexpr uint64_t floor_bytes = 64ull * 1024 * 1024;
    if (target < floor_bytes) {
        why = "target " + std::to_string(target) + " is below the " +
              std::to_string(floor_bytes) + " floor - refusing";
        say_once(why);
        return false;
    }

    std::ofstream out(file, std::ios::out | std::ios::trunc);
    if (!out) {
        why = "cannot open " + file + " for writing - the process needs to own a "
              "delegated cgroup";
        say_once(why);
        return false;
    }
    out << target << "\n";
    out.flush();
    if (!out) {
        why = "writing " + std::to_string(target) + " to " + file + " failed";
        say_once(why);
        return false;
    }
    {
        std::lock_guard lock(written_mut);
        written_file = file;
    }
    why.clear();
    return true;
}

void heap::release_cgroup_memory_max() {
    std::string file;
    {
        std::lock_guard lock(written_mut);
        file = written_file;
        written_file.clear();
    }
    if (file.empty())
        return;                     // never set one, so nothing of ours to undo
    std::ofstream out(file, std::ios::out | std::ios::trunc);
    if (!out) {
        barch::err({"could not reopen", file, "to release the limit barch set"});
        return;
    }
    out << "max\n";
    out.flush();
    if (!out) {
        barch::err({"could not write max back to", file});
        return;
    }
    barch::log({"released the memory limit barch set on", file});
}
