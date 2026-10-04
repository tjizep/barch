// Windows stand-in for glibc's <execinfo.h>. backtrace() is real
// (CaptureStackBackTrace); backtrace_symbols() gives addresses only, since
// there are no symbols in a release exe to look up.
#ifndef BARCH_WIN32_EXECINFO_H
#define BARCH_WIN32_EXECINFO_H

#ifdef __cplusplus
extern "C" {
#endif

int backtrace(void** buffer, int size);
char** backtrace_symbols(void* const* buffer, int size);

#ifdef __cplusplus
}
#endif

#endif
