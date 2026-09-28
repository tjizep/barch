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
// pin_data_dir() takes the current directory, the first time it's called, and
// says where it pinned. The entry points call it as soon as they've settled on a
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
}
