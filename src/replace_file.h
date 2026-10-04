#ifndef BARCH_REPLACE_FILE_H
#define BARCH_REPLACE_FILE_H

#include <cstdio>

namespace barch {
    /**
     * Rename `from` over `to`, replacing `to` if it's there - what std::rename does
     * on Linux, and what every write-a-temp-then-swap-it-in save here relies on.
     * Windows' std::rename refuses when `to` exists, so it goes through MoveFileEx
     * there (win32/src/posix_compat.cpp). 0 on success, like std::rename.
     */
    inline int replace_file(const char* from, const char* to) {
#ifdef _WIN32
        return barch_win32_rename(from, to);
#else
        return std::rename(from, to);
#endif
    }
}

#endif
