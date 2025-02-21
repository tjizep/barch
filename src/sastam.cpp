//
// Created by linuxlite on 2/10/25.
//
#include "valkeymodule.h"
#include "sastam.h"
enum
{
    padding = 2
};
std::atomic<uint64_t> heap::allocated;
#if 0
static uint32_t get_ptr_val(const void* v)
{
    const auto* ptr =(const uint8_t*)v;
    auto ax = (uint64_t)ptr;
    ax &= ((1ll << 30) - 1);
    uint32_t ax32 = ax;
    return ax32;
}
#endif
void* heap::allocate(size_t size){
    if (!size) return nullptr;

    auto* r = ValkeyModule_Calloc(1, size+padding);
    if(r)
    {
#if 0
        uint32_t ax32 = get_ptr_val(r);
        memcpy((uint8_t*)r+size, &ax32, sizeof(ax32));
        check_ptr(r, size);
#endif
        allocated+=size+padding;
    }
    return r;
}
void heap::check_ptr(void* ptr, size_t )
{
    if(!ptr) return;
#if 0
    uint32_t ax32 = get_ptr_val(ptr);
    uint32_t ax32t = 0;
    memcpy(&ax32t, (const uint8_t*)ptr + size, sizeof(ax32));
    if(ax32t != ax32)
    {
        abort();
    }
#endif

}
void heap::free(void* ptr, size_t size){
    if(ptr){
        check_ptr(ptr, size);
        ValkeyModule_Free(ptr);
        allocated -= size+padding;
    }
}

