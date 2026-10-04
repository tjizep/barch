// Included ahead of every barch source in the Windows build (-include, see
// win32/CMakeLists.txt). It covers what glibc gives the code for free and
// MinGW-w64 doesn't, so the shared sources don't need #ifdef _WIN32 for it.
// The functions declared here live in win32/src/posix_compat.cpp.
#ifndef BARCH_WIN32_H
#define BARCH_WIN32_H

// winsock2 has to come before anything that pulls in <windows.h>, or windows.h
// brings the old winsock.h along and the two clash
//
// SIZE and ACL are windows types and barch command handlers. The windows ones
// are renamed while its headers are read - every later include of them is a
// no-op behind its guard - which leaves the plain names to barch.
#define SIZE WIN32_SIZE
#define ACL WIN32_ACL
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <mswsock.h>
#include <psapi.h>
#undef SIZE
#undef ACL

#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <io.h>
#include <direct.h>
#include <unistd.h>
#include <stdio.h>
#include <time.h>

// a glibc decoration on function declarations
#ifndef __THROW
#define __THROW
#endif

// windows has no close-on-exec, but a handle that isn't inherited is the same
// thing for the one place barch starts a child (the git runner)
#ifndef O_CLOEXEC
#define O_CLOEXEC _O_NOINHERIT
#endif
// opening a directory isn't possible through the crt; the one caller, the
// directory fsync in hash_arena, has a windows branch of its own
#ifndef O_DIRECTORY
#define O_DIRECTORY 0
#endif

#ifndef S_ISLNK
#define S_ISLNK(m) 0
#endif

#ifndef _SC_PAGESIZE
#define _SC_PAGESIZE 30
#endif

#ifndef MSG_DONTWAIT
#define MSG_DONTWAIT 0
#endif

// the crt has no synchronous open. queue_file makes its each_add descriptor
// write through with barch_win32_write_through() instead
#ifndef O_DSYNC
#define O_DSYNC 0
#endif

#ifdef __cplusplus
#include <cstddef>
// glibc's headers put nullptr_t in the global namespace and barch leans on it
using std::nullptr_t;

extern "C" {
#endif

int fsync(int fd);
ssize_t pread(int fd, void* buf, size_t count, off_t offset);
ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset);
long sysconf(int name);
// there are no symlinks to tell apart, as far as barch is concerned
int lstat(const char* path, struct stat* st);
// the rename() posix promises: replaces `to` if it's there. The crt's doesn't
int barch_win32_rename(const char* from, const char* to);
// fd reopened with FILE_FLAG_WRITE_THROUGH, which is what O_DSYNC asks for. The
// old descriptor is closed; -1 (and the old one closed) if it can't be done
int barch_win32_write_through(int fd);
// no preallocation through the crt: says so the way linux does on a filesystem
// without it, which the caller already handles
int fallocate(int fd, int mode, off_t offset, off_t len);
char* realpath(const char* path, char* resolved);
void* memmem(const void* haystack, size_t hlen, const void* needle, size_t nlen);

#ifdef __cplusplus
}

// mingw's mkdir takes no mode; windows has no permission bits to give it
inline int mkdir(const char* path, int /*mode*/) { return ::_mkdir(path); }
inline int fdatasync(int fd) { return fsync(fd); }
inline long gettid() { return (long) GetCurrentThreadId(); }
inline void flockfile(FILE* f) { _lock_file(f); }
inline void funlockfile(FILE* f) { _unlock_file(f); }
inline struct tm* gmtime_r(const time_t* t, struct tm* out) {
    return gmtime_s(out, t) == 0 ? out : nullptr;
}
#endif

#endif
