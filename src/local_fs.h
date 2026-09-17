//
// The four things every directory walk in here needs: is it a directory, is it a
// regular file, read the whole thing, list what's in it.
//
// There used to be a copy of all four in fs_api.cpp and another in
// function_sync.cpp. They drifted - one read_file checked for read errors and the
// other could not - and the unity build finally refused to compile them side by
// side. See TODO 361 and DONE 340.
//
// Deliberately thin: no key space, no logging, no configuration. Plain paths on
// the local disk, which is all the callers want.
//
#ifndef BARCH_LOCAL_FS_H
#define BARCH_LOCAL_FS_H

#include <string>
#include <vector>

namespace barch::localfs {
    /** true when `path` exists and is a directory */
    bool is_dir(const std::string& path);

    /** true when `path` exists and is a regular file - a symlink is followed */
    bool is_reg(const std::string& path);

    /**
     * The whole file into `out`. False when it could not be opened, and also
     * false on a read error part way through, which is why this does not use an
     * istreambuf_iterator: that cannot tell a short read from a finished one.
     */
    bool read_file(const std::string& path, std::string& out);

    /**
     * The names in `path`, sorted, without `.` and `..`. Names only, not paths.
     *
     * Sorted so that importing the same tree twice writes in the same order - a
     * failure is then reproducible instead of depending on what the filesystem
     * hands back. Dot files other than the two directory entries are kept; the
     * callers that do not want them say so themselves.
     */
    std::vector<std::string> list_dir(const std::string& path);
}

#endif //BARCH_LOCAL_FS_H
