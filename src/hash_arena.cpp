//
// Created by linuxlite on 3/27/25.
//
#include "hash_arena.h"

#include <filesystem>

#include "art/art.h"
#include "rpc/server.h"
#include "module.h"

void append(std::ostream &out, size_t page, const storage &s, const uint8_t *data) {
    if (out.fail()) {
        barch::err({std::runtime_error("out of disk space or device error").what(), __FILE__, __LINE__});
        return;
    }

    uint32_t size = 0;
    if (s.fragmentation > s.write_position) {
        abort_with("invalid write position or fragmentation");
    }
    writep(out, page); //8
    writep(out, s.fragmentation); //4
    writep(out, (uint32_t)0);//4
    writep(out, s.size);//4
    writep(out, s.ticker);//8
    writep(out, s.write_position);//4
    size = 0;
    writep(out, size);//4
    size = page_size; //8+4+4+4+8+4+4+page_size
    writep(out, size);
    //if (size)
    writep(out, data, size);
}


/// file io
bool arena::base_hash_arena::save(const std::string &filename,
                                  const std::function<void(std::ostream &)> &extra) const {
    if (log_saving_messages == 1)
        barch::log({"writing to " + filename});
    std::string wal_filename = filename + ".wal";
    std::remove(wal_filename.c_str()); // remove wal if its existing
    std::ofstream out{wal_filename, std::ios::out | std::ios::binary}; // the wal file is truncated if it exists
    if (!out.is_open()) {
        barch::err({std::runtime_error("file could not be opened").what(), __FILE__, __LINE__});

        return false;
    }
    uint64_t completed = 0;

    if (!send(out, extra, false)) {
        return false;
    }
    out.seekp(0);
    completed = storage_version;
    writep(out, completed);
    out.flush();
    out.close();
    std::string bak = filename + ".back";
    std::remove(bak.c_str()); // make sure back file is gone
    std::rename(filename.c_str(), bak.c_str());
    std::rename(wal_filename.c_str(), filename.c_str());
    std::remove(bak.c_str()); // remove the old version
    if (log_saving_messages == 1)
        barch::log({"completed writing to " + filename});

    return !out.fail();
}


bool arena::base_hash_arena::send(std::ostream &out, const std::function<void(std::ostream &)> &extra, bool write_version) const {
    uint64_t completed = write_version ? (int)storage_version : 0;
    size_t size = hidden_arena.size();
    writep(out, completed);
    writep(out, max_accessible_page());
    writep(out, last_allocated);
    writep(out, free_pages);
    writep(out, top);
    extra(out);
    writep(out, size);
    if (out.fail()) {
        return false;
    }
    size_t record_pos = 0;
    iterate_arena([&](size_t page, const size_t &) {
        if (free_address_list.contains(page)) {
            abort_with("free page accounting error");
        }
        const storage& s = *(const storage*)get_page_data({page, page_size - sizeof(storage), nullptr},false);
        append(out, page, s, get_page_data({page, 0, nullptr},false));
        ++record_pos;
    });
    if (log_loading_messages == 1) {
        barch::log({"saved [",record_pos,"] out of [",size,"] pages"});
        barch::log({"memory used by data",(double)statistics::logical_allocated/(1024.0*1024.0),"MB"});
    }
    if (out.fail()) {
        barch::err({std::runtime_error("out of disk space or device error").what(), __FILE__, __LINE__});
        return false; // usually disk full at this stage
    }

    return true;
}

bool arena::base_hash_arena::arena_read(base_hash_arena &arena, const std::function<void(std::istream &)> &extra,
                                        const std::string &filename) {
    std::ifstream in{filename, std::ios::in | std::ios::binary};
    if (!in.is_open()) {
        if (log_loading_messages == 1)
            barch::err({std::runtime_error("file could not be opened").what(), __FILE__, __LINE__});
        return false;
    }
    if (log_loading_messages == 1)
        barch::log({"reading from",std::filesystem::current_path().c_str(),filename});
    in.seekg(0, std::ios::end);
    //uint64_t eof = in.tellg();
    in.seekg(0, std::ios::beg);
    arena_retrieve(arena, in, extra);
    if (log_loading_messages == 1)
        barch::log({"complete reading from",std::filesystem::current_path().c_str(),filename});
    return true;
}

bool arena::base_hash_arena::arena_retrieve(base_hash_arena &arena, std::istream& in, const std::function<void(std::istream &)> &extra) {
    uint64_t completed = 0;
    size_t size = 0;
    readp(in, completed);
    if (completed != storage_version) {
        barch::err({std::runtime_error("data format is invalid").what(), __FILE__, __LINE__});

        return false;
    }
    size_t max_address_accessed;
    readp(in, max_address_accessed);
    if (in.fail()) {
        barch::err({std::runtime_error("data could not be accessed").what(), __FILE__, __LINE__});
        return false;
    }
    readp(in, arena.last_allocated);
    // TODO: the free page list is never recovered - although the count is correct
    readp(in, arena.free_pages);
    readp(in, arena.top);
    extra(in);
    //uint64_t where = 0;

    size = 0;
    readp(in, size);
    // TODO: the free page list is never recovered
    bool oom = false;
    for (size_t i = 0; i < size; i++) {
        storage s{};
        size_t page = 0;
        if (arena.is_check_mem() && (statistics::logical_allocated > barch::get_max_module_memory() || heap::get_physical_memory_ratio() > 0.99)) {
            //arena.clear();
            //return false;
            oom = true;
        }
        uint32_t bsize = 0;

        readp(in, page);
        if (page > max_address_accessed) {
            barch::err({"invalid page"});
            return false;
        }
        if (arena.hidden_arena.contains(page)) {
            barch::err({"invalid page - already loaded"});
            return false;
        }
        readp(in, s.fragmentation);
        uint32_t mods = 0;
        readp(in, mods);
        readp(in, s.size);
        readp(in, s.ticker);
        readp(in, s.write_position);
        readp(in, bsize);
        if (bsize) {
            abort_with("invalid file");
        }
        if (s.fragmentation > s.write_position) {
            abort_with("invalid write position or fragmentation");
        }

        readp(in, bsize);
        if (bsize != page_size) {
            abort_with("invalid page size");
        }
        uint8_t* data = arena.get_alloc_page_data({page, 0, nullptr}, bsize);
        readp(in, data, bsize);
        arena.hidden_arena[page] = page;
        arena.max_allocated_page = std::max<size_t>(arena.max_allocated_page, page);
        if (in.fail()) {
            barch::err({std::runtime_error("file could not be accessed").what(), __FILE__, __LINE__});
            return false;
        }
        storage& ps = *(storage*)arena.get_page_data({page,LPageSize,nullptr}, false);
        ps.lru = lru_list::iterator();
    };

    if (!in.eof() && in.fail()) {
        barch::err({std::runtime_error("data could not be accessed").what(), __FILE__, __LINE__});
        return false;
    }
    arena.reconcile_free_list();
    if (log_loading_messages == 1) {
        barch::log({"loaded [",size,"] pages"});
        barch::log({"complete reading from stream"});
        if (oom) {
            barch::log({"loading module or server out of memory"});
        }
    }
    return true;
}


bool arena::base_hash_arena::load(const std::string &filename, const std::function<void(std::istream &)> &extra) {
    base_hash_arena anew_one;
    anew_one.set_check_mem(this->is_check_mem());
    // and where its pages are mapped from, before it allocates any - otherwise a
    // load lands in anonymous memory whatever `arena_dir` says. TODO 239
    anew_one.set_backing_name(this->get_backing_name());
    this->close_backing_file();     // the file is about to be the new arena's
    if (arena_read(anew_one, extra, filename)) {
        *this = std::move(anew_one); // only update if successful
        return true;
    }
    return false;
}
bool arena::base_hash_arena::retrieve(std::istream &in, const std::function<void(std::istream &)> &extra) {
    base_hash_arena anew_one;
    if (arena_retrieve(anew_one, in, extra)) {
        *this = std::move(anew_one); // only update if successful
        return true;
    }
    return false;
}

/*
 * The snapshot beside a mapped arena - TODO 262. See hash_arena.h for what it is
 * for; this is the format.
 *
 * The completion stamp is written last and to the front, the way `save` already
 * does it: a file that was cut short still has a zero there and is refused rather
 * than half believed. Written to a temporary and renamed, so a reader never sees a
 * partial one at all.
 */
std::atomic<uint64_t>& arena::mapped_count() {
    static std::atomic<uint64_t> n{0};
    return n;
}

namespace {
    constexpr uint64_t snapshot_stamp = 0x424152434853ull;   // "BARCHS"

    std::string snapshot_path_of(const std::string& arena_path) {
        return arena_path + ".meta";
    }
}

bool arena::base_hash_arena::save_snapshot(const std::function<void(std::ostream &)> &extra) const {
    if (backing_path.empty() || page_data == nullptr)
        return false;
    const std::string path = snapshot_path_of(backing_path);
    const std::string tmp = path + ".wal";
    std::remove(tmp.c_str());
    std::ofstream out{tmp, std::ios::out | std::ios::binary};
    if (!out.is_open()) {
        barch::err({"could not write the arena snapshot", tmp});
        return false;
    }
    uint64_t completed = 0;
    writep(out, completed);
    uint64_t version = storage_version;
    uint64_t psize = page_size;
    uint64_t bytes = page_data_size;
    writep(out, version);
    writep(out, psize);
    writep(out, bytes);
    uint64_t w_top = top, w_free = free_pages, w_max = max_allocated_page, w_last = last_allocated;
    writep(out, w_top);
    writep(out, w_free);
    writep(out, w_max);
    writep(out, w_last);
    // the same allocator state the shard file carries, through the same writer
    extra(out);
    uint64_t count = hidden_arena.size();
    writep(out, count);
    for (const auto& [at, value] : hidden_arena) {
        uint64_t a = at, v = value;
        writep(out, a);
        writep(out, v);
    }
    if (out.fail()) {
        barch::err({"could not write the arena snapshot", tmp});
        return false;
    }
    out.seekp(0);
    completed = snapshot_stamp;
    writep(out, completed);
    out.flush();
    out.close();
    if (out.fail())
        return false;
    std::remove(path.c_str());
    std::rename(tmp.c_str(), path.c_str());
    return true;
}

bool arena::base_hash_arena::load_snapshot(const std::function<void(std::istream &)> &extra) {
    if (!wants_backing())
        return false;
    auto dir = barch::get_arena_dir();
    const std::string arena_file = dir + "/" + backing_name + ".arena";
    const std::string path = snapshot_path_of(arena_file);

    std::ifstream in{path, std::ios::in | std::ios::binary};
    if (!in.is_open())
        return false;
    /*
     * Read once. Whatever happens next - mapped, refused, or a crash a second
     * later - this file has had its one chance, and start-up after a crash finds
     * nothing to trust.
     */
    std::remove(path.c_str());

    uint64_t completed = 0, version = 0, psize = 0, bytes = 0;
    readp(in, completed);
    if (completed != snapshot_stamp)
        return false;                            // cut short, or not one of ours
    readp(in, version);
    readp(in, psize);
    readp(in, bytes);
    if (version != (uint64_t) storage_version || psize != (uint64_t) page_size || bytes == 0)
        return false;                            // a different build wrote it

    struct stat st{};
    if (::stat(arena_file.c_str(), &st) != 0 || (uint64_t) st.st_size < bytes)
        return false;                            // the pages it describes are not there

    uint64_t w_top = 0, w_free = 0, w_max = 0, w_last = 0;
    readp(in, w_top);
    readp(in, w_free);
    readp(in, w_max);
    readp(in, w_last);
    extra(in);
    uint64_t count = 0;
    readp(in, count);
    hash_type restored;
    restored.reserve(count);
    for (uint64_t i = 0; i < count; ++i) {
        uint64_t a = 0, v = 0;
        readp(in, a);
        readp(in, v);
        restored[a] = v;
    }
    if (in.fail())
        return false;

    // nothing has been changed until here, so a refusal above costs only the load
    // that was going to happen anyway
    // clear() drops the mapping and the path but keeps the name, which is
    // configuration rather than state
    clear();
    if (!prepare_backing(false)) {
        return false;
    }
    int fd = ::open(backing_path.c_str(), O_RDWR);
    if (fd < 0) {
        close_backing();
        return false;
    }
    auto* mapped = (uint8_t*) mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ::close(fd);
    if (mapped == MAP_FAILED) {
        close_backing();
        return false;
    }
    page_data = mapped;
    page_data_size = bytes;
    heap::allocated += bytes;
    heap::vmm_allocated += bytes;
    top = w_top;
    free_pages = w_free;
    max_allocated_page = w_max;
    last_allocated = w_last;
    hidden_arena = std::move(restored);
    // derived, so it is rebuilt rather than stored
    reconcile_free_list();
    modified.resize(page_data_size / physical_page_size + 1);
    page_modifications::inc_all_tickers();
    ++mapped_count();
    return true;
}
