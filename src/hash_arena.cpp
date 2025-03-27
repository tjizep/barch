//
// Created by linuxlite on 3/27/25.
//
#include "hash_arena.h"
#include "art.h"
/// file io
bool hash_arena::save(const std::string& filename, const std::function<void(std::ofstream&)>& extra) const
{
    art::log("writing to " + filename);
    std::ofstream out{filename,std::ios::out|std::ios::binary};
    if (!out.is_open())
    {
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
    writep(out, size);
    extra(out);

    if (out.fail())
    {
        return false;
    }
    iterate_arena([&out](size_t page, const storage& s)
    {
        if (out.fail())
        {
            art::log(std::runtime_error("out of disk space or device error"),__FILE__,__LINE__);
            return;
        }

        long size = 0;
        writep(out,page);
        writep(out,s.fragmentation);
        writep(out,s.modifications);
        writep(out,s.size);
        writep(out,s.ticker);
        writep(out,s.write_position);
        size = s.compressed.byte_size();
        writep(out,size);
        if (size)
            out.write((const char*)s.compressed.begin(), size);
        size = s.decompressed.byte_size();
        writep(out,size);
        if (size)
            out.write((const char*)s.decompressed.begin(), size);

    });
    if (out.fail())
    {
        art::log(std::runtime_error("out of disk space or device error"),__FILE__,__LINE__);
        return false; // usually disk full at this stage
    }
    out.seekp(0);
    completed = storage_version;
    writep(out,completed);
    out.flush();
    art::log("completed writing to " + filename);

    return !out.fail();

}
bool hash_arena::arena_read(hash_arena& arena, const std::function<void(std::ifstream&)>& extra, const std::string& filename)
{
    art::log("reading from " + filename);
    std::ifstream in{filename,std::ios::in|std::ios::binary};
    if(!in.is_open())
    {
        art::log(std::runtime_error("file could not be opened"),__FILE__,__LINE__);
        return false;
    }
    uint64_t completed = 0;
    size_t size = 0;
    in.read((char*)&completed, sizeof(completed));
    if (completed != storage_version)
    {
        art::log(std::runtime_error("file format is invalid"),__FILE__,__LINE__);

        return false;
    }
    in.read((char*)&arena.max_address_accessed, sizeof(arena.max_address_accessed));
    if (in.fail())
    {
        art::log(std::runtime_error("file could not be accessed"),__FILE__,__LINE__);
        return false;
    }
    readp(in,arena.last_allocated);
    readp(in,arena.free_pages);
    readp(in,arena.top);
    readp(in,size);
    extra(in);
    for (size_t i = 0; i < size; i++)
    {
        storage s{};
        size_t page = 0;
        if (heap::allocated > art::get_max_module_memory() || heap::get_physical_memory_ratio() > 0.99)
        {
            art::log(std::runtime_error("module or server out of memory"),__FILE__,__LINE__);
            return false;
        }
        long bsize = 0;
        readp(in,page);
        readp(in,s.fragmentation);
        readp(in,s.modifications);
        readp(in,s.size);
        readp(in,s.ticker);
        readp(in,s.write_position);
        readp(in,bsize);
        if (bsize)
        {
            s.compressed = heap::buffer<uint8_t>(bsize);
            in.read((char*)s.compressed.begin(), bsize);
        }
        readp(in,bsize);
        if (bsize)
        {
            s.decompressed = heap::buffer<uint8_t>(bsize);
            in.read((char*)s.decompressed.begin(), bsize);
        }
        arena.hidden_arena[page] = s;
        if (in.fail())
        {
            art::log(std::runtime_error("file could not be accessed"),__FILE__,__LINE__);
            return false;
        }
    };
    if (in.fail())
    {
        art::log(std::runtime_error("file could not be accessed"),__FILE__,__LINE__);
        return false;
    }
    art::log("complete reading from " + filename);
    return true;
}

bool hash_arena::load(const std::string& filename, const std::function<void(std::ifstream&)>& extra)
{
    hash_arena anew_one;
    if(arena_read(anew_one, extra, filename))
    {
        *this = anew_one; // only update if successfull
        return true;
    }
    return false;
}