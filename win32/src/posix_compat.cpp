// The POSIX calls barch makes that MinGW-w64 doesn't have, written against the
// Windows api. Declared in win32/include. Only built into the Windows barchd.
//
// None of this tries to be a general POSIX layer. Each function covers the way
// barch calls it and says so where that's narrower than the real thing.

#include <winsock2.h>
#include <windows.h>
#include <psapi.h>

#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>

#include "barch_win32.h"
#include "execinfo.h"
#include "sys/mman.h"
#include "sys/sysinfo.h"
#include "sys/syscall.h"

namespace {

// Text mode is the crt's default, and it turns \n into \r\n on the way out and
// stops reading at a ^Z. Every file barch opens is binary, so make that the
// default before anything opens one - this covers open(), fopen() and fstream.
__attribute__((constructor(101))) void binary_by_default() {
    _fmode = _O_BINARY;      // _set_fmode isn't in msvcrt, only in ucrt
}

int errno_from(DWORD e) {
    switch (e) {
        case ERROR_FILE_NOT_FOUND:
        case ERROR_PATH_NOT_FOUND:     return ENOENT;
        case ERROR_ACCESS_DENIED:
        case ERROR_SHARING_VIOLATION:
        case ERROR_LOCK_VIOLATION:     return EACCES;
        case ERROR_ALREADY_EXISTS:
        case ERROR_FILE_EXISTS:        return EEXIST;
        case ERROR_NOT_ENOUGH_MEMORY:
        case ERROR_OUTOFMEMORY:
        case ERROR_COMMITMENT_LIMIT:   return ENOMEM;
        case ERROR_DISK_FULL:
        case ERROR_HANDLE_DISK_FULL:   return ENOSPC;
        case ERROR_INVALID_HANDLE:     return EBADF;
        default:                       return EIO;
    }
}

void set_errno_from_last() {
    errno = errno_from(GetLastError());
}

// What mmap handed out, so munmap, mremap and msync know what's behind an
// address. Windows has no way to ask a view for its mapping object.
struct mapping {
    size_t size{0};
    HANDLE section{nullptr};   // null for an anonymous mapping
    HANDLE file{nullptr};      // our own handle on the file, kept for growing it
};

std::mutex maps_mu;
std::map<void*, mapping> maps;

// the mapping that holds [addr, addr + length), if one does
std::map<void*, mapping>::iterator find_containing(void* addr, size_t length) {
    auto it = maps.upper_bound(addr);
    if (it == maps.begin())
        return maps.end();
    --it;
    auto* base = static_cast<char*>(it->first);
    auto* at = static_cast<char*>(addr);
    if (at >= base && at + length <= base + it->second.size)
        return it;
    return maps.end();
}

void* map_anonymous(size_t length) {
    // committed up front, but windows hands out zero pages on first touch the
    // same way linux does, so this charges commit and not physical memory
    void* p = VirtualAlloc(nullptr, length, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
    if (!p) {
        set_errno_from_last();
        return MAP_FAILED;
    }
    return p;
}

// a view of `length` bytes of `file`, growing the file to that if it's shorter
void* map_file(HANDLE file, size_t length, HANDLE& section_out) {
    const auto size = static_cast<uint64_t>(length);
    HANDLE section = CreateFileMappingA(file, nullptr, PAGE_READWRITE,
                                        static_cast<DWORD>(size >> 32),
                                        static_cast<DWORD>(size & 0xffffffffu), nullptr);
    if (!section) {
        set_errno_from_last();
        return MAP_FAILED;
    }
    void* p = MapViewOfFile(section, FILE_MAP_ALL_ACCESS, 0, 0, length);
    if (!p) {
        set_errno_from_last();
        CloseHandle(section);
        return MAP_FAILED;
    }
    section_out = section;
    return p;
}

void release(void* addr, const mapping& m) {
    if (m.section) {
        UnmapViewOfFile(addr);
        CloseHandle(m.section);
        CloseHandle(m.file);
    } else {
        VirtualFree(addr, 0, MEM_RELEASE);
    }
}

} // namespace

extern "C" {

// Anonymous private mappings and MAP_SHARED mappings of a whole file from
// offset 0 - the two kinds hash_arena makes. The caller may close `fd` straight
// after, as it does on linux: the mapping keeps a handle of its own.
void* mmap(void* /*addr*/, size_t length, int /*prot*/, int flags, int fd, off_t offset) {
    if (length == 0 || offset != 0) {
        errno = EINVAL;
        return MAP_FAILED;
    }
    mapping m;
    m.size = length;
    void* p;
    if (flags & MAP_ANONYMOUS) {
        p = map_anonymous(length);
    } else {
        HANDLE crt = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
        if (crt == INVALID_HANDLE_VALUE) {
            errno = EBADF;
            return MAP_FAILED;
        }
        if (!DuplicateHandle(GetCurrentProcess(), crt, GetCurrentProcess(), &m.file,
                             0, FALSE, DUPLICATE_SAME_ACCESS)) {
            set_errno_from_last();
            return MAP_FAILED;
        }
        p = map_file(m.file, length, m.section);
        if (p == MAP_FAILED) {
            CloseHandle(m.file);
            return MAP_FAILED;
        }
    }
    if (p != MAP_FAILED) {
        std::lock_guard lock(maps_mu);
        maps[p] = m;
    }
    return p;
}

// Whole mappings only; barch never unmaps part of one.
int munmap(void* addr, size_t /*length*/) {
    mapping m;
    {
        std::lock_guard lock(maps_mu);
        auto it = maps.find(addr);
        if (it == maps.end()) {
            errno = EINVAL;
            return -1;
        }
        m = it->second;
        maps.erase(it);
    }
    release(addr, m);
    return 0;
}

// Always moves - windows can't grow a view or an allocation in place - so this
// is only right with MREMAP_MAYMOVE, which is the only way barch calls it.
// A file mapping keeps its bytes because they're in the file; an anonymous one
// is copied, which linux avoids by moving page table entries.
void* mremap(void* old_address, size_t /*old_size*/, size_t new_size, int flags, ...) {
    if (!(flags & MREMAP_MAYMOVE) || new_size == 0) {
        errno = EINVAL;
        return MAP_FAILED;
    }
    /*
     * The whole call holds maps_mu, and the old address leaves the table before
     * its view is dropped. The other order leaves a window where the range is
     * unmapped but still in the table: another thread maps that freed range,
     * takes the stale entry's place, and then this thread erases the new owner's
     * entry - whose next remap finds nothing and barch gives up. Windows needs
     * the care the way linux does not, because a remap always moves and so always
     * frees the old range. TODO 599
     */
    std::lock_guard lock(maps_mu);
    auto it = maps.find(old_address);
    if (it == maps.end()) {
        errno = EINVAL;
        return MAP_FAILED;
    }
    mapping m = it->second;
    mapping next = m;
    next.size = new_size;
    void* p;
    if (m.section) {
        p = map_file(m.file, new_size, next.section);
        if (p == MAP_FAILED)
            return MAP_FAILED;
        maps.erase(it);
        UnmapViewOfFile(old_address);
        CloseHandle(m.section);
    } else {
        p = map_anonymous(new_size);
        if (p == MAP_FAILED)
            return MAP_FAILED;
        std::memcpy(p, old_address, m.size < new_size ? m.size : new_size);
        maps.erase(it);
        VirtualFree(old_address, 0, MEM_RELEASE);
    }
    maps[p] = next;
    return p;
}

int msync(void* addr, size_t length, int flags) {
    HANDLE file = nullptr;
    {
        std::lock_guard lock(maps_mu);
        auto it = find_containing(addr, length);
        if (it == maps.end()) {
            errno = ENOMEM;
            return -1;
        }
        file = it->second.file;
    }
    if (!file)
        return 0;                        // anonymous: nothing to write back to
    if (!FlushViewOfFile(addr, length)) {
        set_errno_from_last();
        return -1;
    }
    // FlushViewOfFile only hands the pages to the cache manager; MS_SYNC wants
    // them on the device, which takes a flush of the file itself
    if ((flags & MS_SYNC) && !FlushFileBuffers(file)) {
        set_errno_from_last();
        return -1;
    }
    return 0;
}

// Which pages of [addr, addr + length) are in this process's working set.
int mincore(void* addr, size_t length, unsigned char* vec) {
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    const size_t page = si.dwPageSize;
    const size_t pages = (length + page - 1) / page;
    std::vector<PSAPI_WORKING_SET_EX_INFORMATION> info(pages);
    for (size_t i = 0; i < pages; ++i)
        info[i].VirtualAddress = static_cast<char*>(addr) + i * page;
    if (!QueryWorkingSetEx(GetCurrentProcess(), info.data(),
                           static_cast<DWORD>(pages * sizeof(info[0])))) {
        set_errno_from_last();
        return -1;
    }
    for (size_t i = 0; i < pages; ++i)
        vec[i] = info[i].VirtualAttributes.Valid ? 1 : 0;
    return 0;
}

// FlushFileBuffers needs write access, and barch syncs files it opened
// O_RDONLY (arena::sync_file), so a read only handle is reopened for the flush.
int fsync(int fd) {
    HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    if (h == INVALID_HANDLE_VALUE) {
        errno = EBADF;
        return -1;
    }
    if (FlushFileBuffers(h))
        return 0;
    if (GetLastError() != ERROR_ACCESS_DENIED) {
        set_errno_from_last();
        return -1;
    }
    char path[MAX_PATH * 4];
    DWORD n = GetFinalPathNameByHandleA(h, path, sizeof(path), FILE_NAME_NORMALIZED);
    if (n == 0 || n >= sizeof(path)) {
        set_errno_from_last();
        return -1;
    }
    HANDLE w = CreateFileA(path, GENERIC_WRITE,
                           FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                           nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (w == INVALID_HANDLE_VALUE) {
        set_errno_from_last();
        return -1;
    }
    const bool ok = FlushFileBuffers(w);
    if (!ok)
        set_errno_from_last();
    CloseHandle(w);
    return ok ? 0 : -1;
}

// Unlike posix, a positioned read on windows moves the file pointer. Nothing in
// barch mixes pread with read on one descriptor, so that doesn't show.
ssize_t pread(int fd, void* buf, size_t count, off_t offset) {
    HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    if (h == INVALID_HANDLE_VALUE) {
        errno = EBADF;
        return -1;
    }
    OVERLAPPED ov{};
    const auto off = static_cast<uint64_t>(offset);
    ov.Offset = static_cast<DWORD>(off & 0xffffffffu);
    ov.OffsetHigh = static_cast<DWORD>(off >> 32);
    DWORD got = 0;
    const DWORD want = count > 0x7fffffff ? 0x7fffffff : static_cast<DWORD>(count);
    if (!ReadFile(h, buf, want, &got, &ov)) {
        if (GetLastError() == ERROR_HANDLE_EOF)
            return 0;
        set_errno_from_last();
        return -1;
    }
    return static_cast<ssize_t>(got);
}

ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset) {
    HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    if (h == INVALID_HANDLE_VALUE) {
        errno = EBADF;
        return -1;
    }
    OVERLAPPED ov{};
    const auto off = static_cast<uint64_t>(offset);
    ov.Offset = static_cast<DWORD>(off & 0xffffffffu);
    ov.OffsetHigh = static_cast<DWORD>(off >> 32);
    DWORD put = 0;
    const DWORD want = count > 0x7fffffff ? 0x7fffffff : static_cast<DWORD>(count);
    if (!WriteFile(h, buf, want, &put, &ov)) {
        set_errno_from_last();
        return -1;
    }
    return static_cast<ssize_t>(put);
}

long sysconf(int name) {
    if (name == _SC_PAGESIZE) {
        SYSTEM_INFO si;
        GetSystemInfo(&si);
        return static_cast<long>(si.dwPageSize);
    }
    errno = EINVAL;
    return -1;
}

int lstat(const char* path, struct stat* st) {
    return stat(path, st);
}

int barch_win32_rename(const char* from, const char* to) {
    if (MoveFileExA(from, to, MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
        return 0;
    set_errno_from_last();
    return -1;
}

int barch_win32_write_through(int fd) {
    HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    if (h == INVALID_HANDLE_VALUE) {
        errno = EBADF;
        return -1;
    }
    HANDLE w = ReOpenFile(h, GENERIC_READ | GENERIC_WRITE,
                          FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                          FILE_FLAG_WRITE_THROUGH);
    if (w == INVALID_HANDLE_VALUE) {
        set_errno_from_last();
        _close(fd);
        return -1;
    }
    _close(fd);
    const int nfd = _open_osfhandle(reinterpret_cast<intptr_t>(w), _O_RDWR | _O_BINARY);
    if (nfd < 0)
        CloseHandle(w);
    return nfd;
}

int fallocate(int /*fd*/, int /*mode*/, off_t /*offset*/, off_t /*len*/) {
    errno = EOPNOTSUPP;
    return -1;
}

char* realpath(const char* path, char* resolved) {
    char* out = _fullpath(resolved, path, resolved ? MAX_PATH : 0);
    if (!out)
        return nullptr;
    // linux fails on a path that isn't there, and local_fs relies on it
    struct stat st;
    if (stat(out, &st) != 0) {
        if (!resolved)
            std::free(out);
        errno = ENOENT;
        return nullptr;
    }
    for (char* c = out; *c; ++c)
        if (*c == '\\')
            *c = '/';
    return out;
}

void* memmem(const void* haystack, size_t hlen, const void* needle, size_t nlen) {
    if (nlen == 0)
        return const_cast<void*>(haystack);
    if (hlen < nlen)
        return nullptr;
    const auto* h = static_cast<const unsigned char*>(haystack);
    const auto* n = static_cast<const unsigned char*>(needle);
    const unsigned char* last = h + (hlen - nlen);
    for (const unsigned char* p = h; p <= last; ++p) {
        p = static_cast<const unsigned char*>(std::memchr(p, n[0], (size_t) (last - p) + 1));
        if (!p)
            return nullptr;
        if (std::memcmp(p, n, nlen) == 0)
            return const_cast<unsigned char*>(p);
    }
    return nullptr;
}

int sysinfo(struct sysinfo* info) {
    MEMORYSTATUSEX ms{};
    ms.dwLength = sizeof(ms);
    if (!GlobalMemoryStatusEx(&ms)) {
        set_errno_from_last();
        return -1;
    }
    info->totalram = ms.ullTotalPhys;
    info->freeram = ms.ullAvailPhys;
    info->mem_unit = 1;
    return 0;
}

long syscall(long number, ...) {
    if (number == SYS_gettid)
        return static_cast<long>(GetCurrentThreadId());
    errno = ENOSYS;
    return -1;
}

int backtrace(void** buffer, int size) {
    if (size <= 0)
        return 0;
    // skip this frame, the way glibc's doesn't show itself either
    return RtlCaptureStackBackTrace(1, static_cast<DWORD>(size), buffer, nullptr);
}

// One malloc'd block holding the pointer array and the strings after it, so the
// caller's single free() releases the lot - the same contract as glibc's.
//
// Each line is module+offset as well as the address: a DLL like _barch.pyd is
// seldom loaded at its preferred base, so the bare address can't be looked up.
// The offset can - add it to the ImageBase objdump -p reports and hand that to
// addr2line, against a build with -g.
char** backtrace_symbols(void* const* buffer, int size) {
    if (size <= 0)
        return nullptr;
    constexpr size_t each = 160;
    auto** out = static_cast<char**>(std::malloc(size * (sizeof(char*) + each)));
    if (!out)
        return nullptr;
    char* text = reinterpret_cast<char*>(out + size);
    for (int i = 0; i < size; ++i) {
        out[i] = text + i * each;
        const auto at = reinterpret_cast<uintptr_t>(buffer[i]);
        HMODULE mod = nullptr;
        char path[MAX_PATH] = "";
        if (GetModuleHandleExA(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
                               GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                               reinterpret_cast<LPCSTR>(buffer[i]), &mod) &&
            GetModuleFileNameA(mod, path, sizeof(path)) > 0) {
            const char* name = std::strrchr(path, '\\');
            name = name ? name + 1 : path;
            std::snprintf(out[i], each, "%s+0x%llx (0x%016llx)", name,
                          static_cast<unsigned long long>(at - reinterpret_cast<uintptr_t>(mod)),
                          static_cast<unsigned long long>(at));
        } else {
            std::snprintf(out[i], each, "0x%016llx", static_cast<unsigned long long>(at));
        }
    }
    return out;
}

} // extern "C"

/*
 * thread_local destructors ran on freed memory - TODO 599.
 *
 * MinGW gcc has no native TLS, so a thread_local lives in a block libgcc's emutls
 * allocates per thread, and libgcc frees those blocks from a winpthreads key
 * destructor. libstdc++ runs the C++ destructors of thread_locals from a key
 * destructor of its own. winpthreads calls key destructors in key order, and
 * emutls always makes its key first: a thread_local's address is needed before it
 * can be constructed, so before libstdc++ is asked to destroy it. So every thread
 * that exits frees its thread_locals' storage and then runs their destructors in
 * it. A destructor that frees what it owns then frees pointers read out of a freed
 * block, and the heap is corrupted - which surfaces anywhere later: "memory check
 * failed" in a thread_local composite, a crash in emutls's own free, or in
 * ~resp_session when the server stops. A twenty line program with a 4KB
 * thread_local and three threads shows it under wine.
 *
 * The fix is to have libstdc++ make its key first. Registering one destructor
 * before anything touches a thread_local does that, and costs one list entry on
 * the thread that loads this module. Priority 101 is the first a program may use,
 * ahead of every ordinary static initializer, which is where the first thread_local
 * could otherwise be touched.
 */
extern "C" int __cxa_thread_atexit(void (*)(void*), void*, void*);
extern "C" char __dso_handle;

namespace {
char tls_order_anchor;
void tls_order_noop(void*) {}

__attribute__((constructor(101))) void claim_thread_exit_key_first() {
    __cxa_thread_atexit(tls_order_noop, &tls_order_anchor, &__dso_handle);
}
} // namespace
