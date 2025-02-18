//
// Created by linuxlite on 2/10/25.
//
#include "valkeymodule.h"
#include "sastam.h"
std::atomic<uint64_t> heap::allocated;

void* heap::allocate(size_t size){
    if (!size) return nullptr;

    void* r = ValkeyModule_Calloc(1, size +8);
    if(r) allocated+=size;
    return r;
}
void heap::free(void* ptr, size_t size){
    if(ptr){
        ValkeyModule_Free(ptr);
        allocated -= size;
    }
}

