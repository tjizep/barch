#pragma once
//
// The file store, as paths rather than as keys. See TODO 256.
//
// Before this, four places knew the layout: fs_api.cpp, a second metadata parser in
// http_api.cpp, a handler in the browser example, and the fixtures in fstest.py.
// Everything that touches a stored file goes through here now.
//
// THE LAYOUT
//
//     fs:n:<path>          name  -> {id, size, type, version}
//     fs:n:<path>/         a directory: {dir:true}, no id
//     fs:i:<id>            inode -> {size, chunk, chunks, type, version}
//     fs:c:<id>:<n>        one chunk; id and n are both fixed width hex
//     fs:layout            "2", so an old store is refused rather than half read
//
// The data is keyed by an id, not by the path. That is what stops a read rebuilding
// a key carrying the whole path on every chunk, what makes a rename a rewrite of
// name records only - moving a large directory writes a few hundred bytes and
// touches no chunk - and what removes `|` as a reserved character, since no part of
// a chunk key comes from user text any more.
//
// THE ONE INVARIANT: the inode is the truth and the name record is a hint. `size`,
// `type` and `version` are in both so that a listing does not need an inode read
// per entry, and the hint is rewritten on every commit. A writer that updates one
// and not the other has broken this.
//
#include <cstdint>
#include <string>
#include <vector>

#include "foreign/driver.h"
#include "key_space.h"
#include "sastam.h"

namespace barch::fs {

using file_id = uint64_t;
using access = barch::foreign::store_access;

struct entry {
    /** the last segment, which is what a listing shows */
    std::string name;
    /** normalised, absolute, no trailing slash */
    std::string path;
    bool dir{false};
    file_id id{0};
    uint64_t size{0};
    uint64_t chunk{0};
    uint64_t chunks{0};
    uint64_t version{0};
    std::string type;
    /**
     * The source says this exists and it has not been fetched, so there is no size
     * and no version - only a name. `FS LS` calls it `remote`, because a caller that
     * cannot tell it from an empty file will treat it as one.
     */
    bool remote{false};
    /**
     * This file came from the space's source and the source can produce it again,
     * which is the only thing that makes it safe to evict. A file written by
     * LOADFS, FS PUT or a script is the only copy there is and is never a
     * candidate. See TODO 263.
     */
    bool sourced{false};
};

/**
 * Absolute, '/' separated, no `..`, no NUL, no empty segment, no trailing slash.
 * False fills `err` with what is wrong with it.
 */
bool normalise(const std::string& in, std::string& out, std::string& err);

/**
 * A content type guessed from the extension, for a file stored without one.
 * Deliberately short: what a browser needs to render a page and refuse to sniff.
 * Here rather than in the HTTP code because the file store is what decides what a
 * stored file is, and an import and a source both have to agree with a route.
 */
std::string type_from_path(const std::string& path);

/** the keys, exposed because the tests and the importers name them */
std::string name_key(const std::string& path);
std::string inode_key(file_id id);
std::string chunk_key(file_id id, uint64_t n);

/**
 * The name record, without touching the inode. False is "no such path".
 *
 * This is the cheap one, and what a listing uses: it answers size, type and version
 * from the hint the name record carries. It does *not* answer `chunk` or `chunks`,
 * which only the inode has - `stat_full` is for a caller that needs those.
 */
bool stat(const access& acc, const std::string& path, entry& out);
/** the name record and the inode behind it: everything, at the cost of one more get */
bool stat_full(const access& acc, const std::string& path, entry& out);
bool resolve(const access& acc, const std::string& path, file_id& out);
/** the whole file. False is no such file, or a chunk the metadata promised is gone */
bool read(const access& acc, const std::string& path, std::string& out, entry& meta);

/**
 * An open file, holding an id rather than a path: after `open` the path is never
 * walked again, and `open_id` never sees one. It carries the version it opened at,
 * so a read that finds the file rewritten underneath says so rather than splicing
 * two versions together.
 */
class file {
public:
    static bool open(const access& acc, const std::string& path, file& out, std::string& err);
    static bool open_id(const access& acc, file_id id, file& out, std::string& err);
    const entry& meta() const { return e; }
    file_id id() const { return e.id; }
    /** bytes [at, at+len), clipped to the end of the file */
    bool read_at(uint64_t at, uint64_t len, std::string& out, std::string& err) const;
private:
    access acc{};
    entry e{};
};

/**
 * `stat`, and on a miss the space's file source - TODO 263.
 *
 * A space can name a stored function in `fs_source` that produces a file by path.
 * When one is missing this asks for it, writes what comes back as a whole file
 * through a single batch, and answers from that; without a source it is `stat`.
 *
 * A separate call, not something `stat` does quietly, for the same reason
 * `store.fetch` is separate from `store.get` (DONE 252): this one can take a
 * source's latency, and in an HTTP handler it holds a VM slot while it waits. A
 * read that does that should say so where it is written.
 *
 * A miss is remembered for the space's `missing_ttl`, so a file the source does not
 * have does not become a round trip per request.
 */
bool fetch(const key_space_ptr& space, const std::string& path, entry& out,
           std::string& err);

/** whether this space has anywhere to fetch a missing file from */
bool has_source(const key_space_ptr& space);

/**
 * Drop fetched files until the space is inside `fs_cache_bytes` - see TODO 263.
 *
 * Only files that came from the source, oldest fetch first. That is FIFO and not
 * LRU, said plainly: a true LRU wants the read path to write, and a read here goes
 * through `store_access` with no space to write against and no wish to turn every
 * read into one.
 *
 * Returns how many went. A budget of 0 is no eviction, which is what a space that
 * has not asked for one gets.
 */
size_t evict_to_budget(const key_space_ptr& space);

/** what the fetched files in this space add up to */
uint64_t cached_bytes(const key_space_ptr& space);

/**
 * One level of a directory, in name order. `after` continues a listing and `limit`
 * caps it; 0 is no cap.
 *
 * It costs one seek per child rather than one read per descendant: having seen a
 * child directory, the scan jumps past its whole subtree instead of reading it.
 */
bool list(const access& acc, const std::string& dir, std::vector<entry>& out,
          const std::string& after = {}, size_t limit = 0);

/**
 * The same, plus whatever the space's `fs_source_list` says could be there - see
 * TODO 263. What is stored comes back as it is; what only the source knows about is
 * marked `remote` and has a name and nothing else.
 *
 * Not cached: a listing is a question about the source's present state, and the
 * answer stops being true the moment it is stored. A caller that wants it kept can
 * keep it.
 */
bool list_with_source(const key_space_ptr& space, const std::string& dir,
                      std::vector<entry>& out, const std::string& after = {},
                      size_t limit = 0);

/** every path under `dir`, files only, for a caller that wants the subtree */
bool walk(const access& acc, const std::string& dir, std::vector<std::string>& out);

/**
 * Files written together, or not at all. Ids for the new ones are reserved once,
 * before any latch is taken - see ids.h for why that ordering is not optional.
 *
 * The whole content of every file is held until commit, which is what the importers
 * already did: a directory that cannot be read must not leave half an import behind.
 */
class batch {
public:
    explicit batch(const key_space_ptr& space);
    /** `chunk` of 0 takes the default */
    void write(const std::string& path, std::string body, const std::string& type,
               size_t chunk = 0);
    /**
     * The same, marked as having come from the space's source - so it is countable
     * against a cache budget and evictable. Only `fs::fetch` uses this; an ordinary
     * write clears the mark, because what it wrote is now the only copy.
     */
    void write_sourced(const std::string& path, std::string body, const std::string& type,
                       size_t chunk = 0);
    /** an empty directory, which is the only kind that needs saying out loud */
    void mkdir(const std::string& path);
    /** a file, or a directory marker; a directory's contents are not touched */
    void erase(const std::string& path);
    /**
     * Move one name record to another path. The inode and the chunks do not move,
     * which is the whole point: renaming a directory is this, once per name under
     * it, and no file data is touched at all.
     */
    void relink(const std::string& from, const std::string& to);
    bool commit(std::string& err);

    size_t files() const { return pending.size(); }
    uint64_t bytes() const { return staged_bytes; }
    /** the normalised paths written, once commit has succeeded */
    const std::vector<std::string>& written() const { return done; }

private:
    struct item {
        std::string path;
        std::string body;
        std::string type;
        size_t chunk{0};
        bool erase{false};
        bool mkdir{false};
        bool relink{false};
        bool sourced{false};
    };
    key_space_ptr space;
    std::vector<item> pending;
    std::vector<std::string> done;
    uint64_t staged_bytes{0};
};

/** remove one file and its chunks. Returns false when there was nothing there */
bool erase(const key_space_ptr& space, const std::string& path, std::string& err);

/**
 * Make a directory that has nothing in it.
 *
 * Every other directory needs no making: one exists because paths hang under it, so
 * `/a/b` is a directory the moment `/a/b/c` is written and stops being one when the
 * last thing under it goes. A marker is how an empty one is said out loud, and the
 * only thing it changes is that the directory survives being empty.
 *
 * Parents are not required and not created, because there is nothing to create -
 * an unmarked parent of a marked child is already a directory by the same rule.
 */
bool mkdir(const key_space_ptr& space, const std::string& path, std::string& err);

/**
 * Remove a directory. Without `recursive` it refuses one that still has anything
 * in it, marker or not. With it, everything underneath goes too.
 */
bool rmdir(const key_space_ptr& space, const std::string& path, bool recursive,
           std::string& err);

/** whether anything is stored under `path` - a file, a marker, or a deeper path */
bool has_children(const access& acc, const std::string& path);

/**
 * Move a file or a whole directory. One batch, so it lands or it does not.
 *
 * Only name records move: a directory of a thousand files is a thousand small
 * writes and not one byte of content, because the chunks hang off ids and the ids
 * do not change. Refused when the destination exists, and when it is inside the
 * source - a directory cannot be moved into itself.
 */
bool rename(const key_space_ptr& space, const std::string& from, const std::string& to,
            std::string& err);

/**
 * Copy a file or a whole directory. Unlike a move this really does duplicate the
 * content, since a file has one name and no reference count - so it costs what the
 * data costs, and a caller moving a large tree should be moving it.
 */
bool copy(const key_space_ptr& space, const std::string& from, const std::string& to,
          std::string& err);

/**
 * Remove files under `root` that are not in `keep` - what a repository imported as
 * a file store needs after a sync, since the checkout is the truth.
 */
size_t drop_missing(const key_space_ptr& space, const std::string& root,
                    const std::vector<std::string>& keep);

/** the default chunk: well under maximum_allocation_size, a round number of pages */
constexpr size_t default_chunk = 65536;

}
