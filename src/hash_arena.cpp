//
// Created by linuxlite on 3/27/25.
//
#include "hash_arena.h"

#include <filesystem>

#include "art.h"
#include "rpc/server.h"
#include "module.h"

void append(std::ostream &out, size_t page, const storage &s, const uint8_t *data) {
    if (out.fail()) {
        barch::log(std::runtime_error("out of disk space or device error"),__FILE__,__LINE__);
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
    barch::log("writing to " + filename);
    std::string wal_filename = filename + ".wal";
    std::ofstream out{wal_filename, std::ios::out | std::ios::binary}; // the wal file is truncated if it exists
    if (!out.is_open()) {
        barch::log(std::runtime_error("file could not be opened"),__FILE__,__LINE__);

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
    barch::log("completed writing to " + filename);

    return !out.fail();
}


bool arena::base_hash_arena::send(std::ostream &out, const std::function<void(std::ostream &)> &extra, bool write_version) const {
    uint64_t completed = write_version ? (int)storage_version : 0;
    size_t size = hidden_arena.size();
    writep(out, completed);
    writep(out, max_address_accessed);
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
        const storage& s = *(const storage*)get_page_data({page, page_size - sizeof(storage), nullptr},false);
        append(out, page, s, get_page_data({page, 0, nullptr},false));
        ++record_pos;
    });
    barch::std_log("saved [",record_pos,"] out of [",size,"] pages");
    barch::std_log("memory used by data",(double)statistics::logical_allocated/(1024.0*1024.0),"MB");
    if (out.fail()) {
        barch::log(std::runtime_error("out of disk space or device error"),__FILE__,__LINE__);
        return false; // usually disk full at this stage
    }

    return true;
}

bool arena::base_hash_arena::arena_read(base_hash_arena &arena, const std::function<void(std::istream &)> &extra,
                                        const std::string &filename) {
    std::ifstream in{filename, std::ios::in | std::ios::binary};
    if (!in.is_open()) {
        //barch::log(std::runtime_error("file could not be opened"),__FILE__,__LINE__);
        return false;
    }
    barch::std_log("reading from",std::filesystem::current_path().c_str(),filename);
    in.seekg(0, std::ios::end);
    //uint64_t eof = in.tellg();
    in.seekg(0, std::ios::beg);
    arena_retrieve(arena, in, extra);
    barch::std_log("complete reading from",std::filesystem::current_path().c_str(),filename);
    return true;
}

bool arena::base_hash_arena::arena_retrieve(base_hash_arena &arena, std::istream& in, const std::function<void(std::istream &)> &extra) {
    uint64_t completed = 0;
    size_t size = 0;
    readp(in, completed);
    if (completed != storage_version) {
        barch::log(std::runtime_error("data format is invalid"),__FILE__,__LINE__);

        return false;
    }
    readp(in, arena.max_address_accessed);
    if (in.fail()) {
        barch::log(std::runtime_error("data could not be accessed"),__FILE__,__LINE__);
        return false;
    }
    readp(in, arena.last_allocated);
    readp(in, arena.free_pages);
    readp(in, arena.top);
    extra(in);
    //uint64_t where = 0;

    size = 0;
    readp(in, size);

    for (size_t i = 0; i < size; i++) {
        storage s{};
        size_t page = 0;
        if (arena.is_check_mem() && (get_total_memory() > barch::get_max_module_memory() || heap::get_physical_memory_ratio() > 0.99)) {
            barch::log(std::runtime_error("module or server out of memory"),__FILE__,__LINE__);
            return false;
        }
        uint32_t bsize = 0;

        readp(in, page);
        if (page > arena.max_address_accessed) {
            barch::std_err("invalid page");
            return false;
        }
        if (arena.hidden_arena.contains(page)) {
            barch::std_err("invalid page - already loaded");
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
        if (in.fail()) {
            barch::log(std::runtime_error("file could not be accessed"),__FILE__,__LINE__);
            return false;
        }
        storage& ps = *(storage*)arena.get_page_data({page,LPageSize,nullptr}, false);
        ps.lru = lru_list::iterator();
    };

    if (!in.eof() && in.fail()) {
        barch::log(std::runtime_error("data could not be accessed"),__FILE__,__LINE__);
        return false;
    }
    barch::std_log("loaded [",size,"] pages");
    barch::log("complete reading from stream" );
    return true;
}


bool arena::base_hash_arena::load(const std::string &filename, const std::function<void(std::istream &)> &extra) {
    base_hash_arena anew_one;
    anew_one.set_check_mem(this->is_check_mem());
    if (arena_read(anew_one, extra, filename)) {
        *this = anew_one; // only update if successful
        return true;
    }
    return false;
}
bool arena::base_hash_arena::retrieve(std::istream &in, const std::function<void(std::istream &)> &extra) {
    base_hash_arena anew_one;
    if (arena_retrieve(anew_one, in, extra)) {
        *this = anew_one; // only update if successful
        return true;
    }
    return false;
}
