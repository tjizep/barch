// Windows stand-in for <sys/sysinfo.h>. Only the memory fields are filled in,
// which is all sastam.cpp reads.
#ifndef BARCH_WIN32_SYS_SYSINFO_H
#define BARCH_WIN32_SYS_SYSINFO_H

#include <stdint.h>

struct sysinfo {
    uint64_t totalram;
    uint64_t freeram;
    uint32_t mem_unit;
};

#ifdef __cplusplus
extern "C" {
#endif

int sysinfo(struct sysinfo* info);

#ifdef __cplusplus
}
#endif

#endif
