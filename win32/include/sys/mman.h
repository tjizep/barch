// Windows stand-in for <sys/mman.h>, enough for hash_arena: anonymous private
// mappings, MAP_SHARED mappings of a file, mremap with MREMAP_MAYMOVE, msync and
// mincore. Implemented in win32/src/posix_compat.cpp on top of VirtualAlloc and
// file mapping objects.
#ifndef BARCH_WIN32_SYS_MMAN_H
#define BARCH_WIN32_SYS_MMAN_H

#include <stddef.h>
#include <sys/types.h>

#define PROT_NONE  0x0
#define PROT_READ  0x1
#define PROT_WRITE 0x2
#define PROT_EXEC  0x4

#define MAP_SHARED    0x01
#define MAP_PRIVATE   0x02
#define MAP_ANONYMOUS 0x20
#define MAP_ANON      MAP_ANONYMOUS
#define MAP_FAILED    ((void*) -1)

#define MREMAP_MAYMOVE 1

#define MS_ASYNC      1
#define MS_INVALIDATE 2
#define MS_SYNC       4

#ifdef __cplusplus
extern "C" {
#endif

void* mmap(void* addr, size_t length, int prot, int flags, int fd, off_t offset);
int munmap(void* addr, size_t length);
void* mremap(void* old_address, size_t old_size, size_t new_size, int flags, ...);
int msync(void* addr, size_t length, int flags);
int mincore(void* addr, size_t length, unsigned char* vec);

#ifdef __cplusplus
}
#endif

#endif
