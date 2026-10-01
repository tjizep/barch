#pragma once
//
// Where barch keeps its files - TODO 526.
//
// Shard files, dictionaries and the replication files are named relative to the
// working directory, and a relative name is resolved again on every open. So when
// the directory changes under a running process - `CONFIG SET dir` in Valkey, an
// `os.chdir()` in Python - the next save goes somewhere else, while the change log,
// which keeps its file open, stays where it was. Its checkpoint then says the save
// holds everything, the trim drops the records, and a restart in the first
// directory finds neither: writes made after the move are gone.
//
// So the directory is taken once, as an absolute path, and every data file is
// named from it. Moving the working directory afterwards doesn't move barch's
// files; they stay together where they started.
//
// pin_data_dir() takes the current directory, the first time it's called, holds
// it for this process (hold_dir, TODO 571), and says where it pinned. The entry points call it as soon as they've settled on a
// directory (barchd after --dir, the Valkey module when it loads, the Python
// start). data_dir() pins on first use too, for whatever gets there first.
//
#include <string>

namespace barch {
    /** take the current directory as the data directory, unless one is taken already */
    void pin_data_dir();
    /** the data directory, absolute, without a trailing slash */
    const std::string& data_dir();
    /**
     * `path` inside the data directory. An absolute path is returned as it is, so a
     * setting that names a directory of its own keeps meaning it.
     */
    std::string data_path(const std::string& path);

    /**
     * Hold `dir` for this process, so no other barch process can use it - TODO 571.
     *
     * Two processes on one directory overwrite each other's saves, and each one's
     * startup removes `.wal` files the other is still writing. This takes flock on
     * the directory itself, so there's no lock file to leave behind, and the kernel
     * drops it when the process goes, however it goes.
     *
     * True when this process holds `dir` - now, or from an earlier call for the same
     * directory under any name. False, with `err` naming the holder, when another
     * process does. A file system that can't lock at all is logged and taken as held:
     * it's no worse than before.
     */
    bool hold_dir(const std::string& dir, std::string& err);

    /**
     * Whether this process holds its data directory. pin_data_dir() asks for it, so
     * this answers what that got; `err` says why not.
     */
    bool data_dir_held(std::string& err);
}
