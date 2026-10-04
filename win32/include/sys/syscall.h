// Windows stand-in for <sys/syscall.h>. The only call barch makes is
// syscall(SYS_gettid), for the thread ids in the latch diagnostics.
#ifndef BARCH_WIN32_SYS_SYSCALL_H
#define BARCH_WIN32_SYS_SYSCALL_H

#define SYS_gettid 186

#ifdef __cplusplus
extern "C" {
#endif

long syscall(long number, ...);

#ifdef __cplusplus
}
#endif

#endif
