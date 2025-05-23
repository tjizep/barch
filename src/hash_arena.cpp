//
// Created by linuxlite on 3/27/25.
//
#include "hash_arena.h"
#include "art.h"

void append(std::ofstream &out, size_t page, const storage &s, const uint8_t *data) {
    if (out.fail()) {
        art::log(std::runtime_error("out of disk space or device error"),__FILE__,__LINE__);
        return;
    }

    long size = 0;
    if (s.fragmentation > s.write_position) {
        abort_with("invalid write position or fragmentation");
    }
    writep(out, page);
    writep(out, s.fragmentation);
    writep(out, 0);
    writep(out, s.size);
    writep(out, s.ticker);
    writep(out, s.write_position);
    size = 0;
    writep(out, size);
    size = page_size; //s.write_position; //s.decompressed.byte_size();
    writep(out, size);
    //if (size)
    out.write((const char *) data, size);
}

static const uint64_t alloc_record_size = sizeof(uint64_t) * 2;

/// file io
bool arena::base_hash_arena::save(const std::string &filename,
                                  const std::function<void(std::ofstream &)> &extra) const {
    art::log("writing to " + filename);
    std::ofstream out{filename, std::ios::out | std::ios::binary};
    if (!out.is_open()) {
        art::log(std::runtime_error("file could not be opened"),__FILE__,__LINE__);

        return false;
    }
    uint64_t completed = 0;
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
    uint64_t alloc_table_start = out.tellp();
    // write the initial allocation table
    size_t page_count = 0;
    iterate_arena([&out,&page_count](size_t page, const size_t &) {
        uint64_t p = page;
        uint64_t a = 0;

        writep(out, p);
        writep(out, a);
        ++page_count;
    });
    size_t record_pos = 0;
    iterate_arena([&](size_t page, const size_t &) {
        uint64_t start = out.tellp();
        const storage& s = *(const storage*)get_page_data({page, page_size - sizeof(storage), nullptr},false);
        append(out, page, s, get_page_data({page, 0, nullptr},false));

        uint64_t finish = out.tellp();
        // write the allocation record
        out.seekp(alloc_table_start + alloc_record_size * record_pos + sizeof(uint64_t), std::ios::beg);
        writep(out, start);
        out.seekp(finish, std::ios::beg);
        ++record_pos;
    });
    art::std_log("saved [",record_pos,"] out of [",size,"] pages");
    if (out.fail()) {
        art::log(std::runtime_error("out of disk space or device error"),__FILE__,__LINE__);
        return false; // usually disk full at this stage
    }
    out.seekp(0);
    completed = storage_version;
    writep(out, completed);
    out.flush();

    art::log("completed writing to " + filename);

    return !out.fail();
}

bool arena::base_hash_arena::arena_read(base_hash_arena &arena, const std::function<void(std::ifstream &)> &extra,
                                        const std::string &filename) {
    art::log("reading from " + filename);
    std::ifstream in{filename, std::ios::in | std::ios::binary};
    if (!in.is_open()) {
        art::log(std::runtime_error("file could not be opened"),__FILE__,__LINE__);
        return false;
    }
    in.seekg(0, std::ios::end);
    uint64_t eof = in.tellg();
    in.seekg(0, std::ios::beg);
    uint64_t completed = 0;
    size_t size = 0;
    in.read((char *) &completed, sizeof(completed));
    if (completed != storage_version) {
        art::log(std::runtime_error("file format is invalid"),__FILE__,__LINE__);

        return false;
    }
    in.read((char *) &arena.max_address_accessed, sizeof(arena.max_address_accessed));
    if (in.fail()) {
        art::log(std::runtime_error("file could not be accessed"),__FILE__,__LINE__);
        return false;
    }
    readp(in, arena.last_allocated);
    readp(in, arena.free_pages);
    readp(in, arena.top);
    extra(in);
    uint64_t where = 0;
    do {
        size = 0;
        readp(in, size);
        uint64_t alloc_table_start = in.tellg();
        in.seekg(alloc_table_start + size * alloc_record_size);

        for (size_t i = 0; i < size; i++) {
            storage s{};
            size_t page = 0;
            if (heap::allocated > art::get_max_module_memory() || heap::get_physical_memory_ratio() > 0.99) {
                art::log(std::runtime_error("module or server out of memory"),__FILE__,__LINE__);
                return false;
            }
            long bsize = 0;

            uint64_t start = in.tellg();
            readp(in, page);
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
            in.read((char *) data, bsize);
            arena.hidden_arena[page] = page;
            if (in.fail()) {
                art::log(std::runtime_error("file could not be accessed"),__FILE__,__LINE__);
                return false;
            }
            uint64_t finish = in.tellg();
            // write the allocation record
            in.seekg(alloc_table_start + alloc_record_size * i);
            uint64_t tp = 0;

            readp(in, tp);
            if (tp != page) {
                art::log(std::runtime_error("invalid or incomplete BARCH data file"),__FILE__,__LINE__);
                return false;
            }
            readp(in, s.physical);
            if (s.physical != start) {
                art::log(std::runtime_error("invalid or incomplete BARCH data file"),__FILE__,__LINE__);
                return false;
            }
            in.seekg(finish, std::ios::beg);
        };
        where = in.tellg();
    } while (eof > where);

    if (!in.eof() && in.fail()) {
        art::log(std::runtime_error("file could not be accessed"),__FILE__,__LINE__);
        return false;
    }
    art::std_log("loaded [",size,"] pages");
    art::log("complete reading from " + filename);
    return true;
}

bool arena::base_hash_arena::load(const std::string &filename, const std::function<void(std::ifstream &)> &extra) {
    base_hash_arena anew_one;
    if (arena_read(anew_one, extra, filename)) {
        *this = anew_one; // only update if successfull
        return true;
    }
    return false;
}
