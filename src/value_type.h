//
// Created by linuxlite on 3/22/25.
//

#ifndef VALUE_TYPE_H
#define VALUE_TYPE_H
#include <cstddef>
#include <stdexcept>
namespace art
{
    struct value_type
    {
        const unsigned char* bytes;
        unsigned size;
        value_type(const char * v, unsigned l): bytes((const unsigned char*)v), size(l) {} ;
        value_type(const unsigned char * v, unsigned l): bytes(v), size(l) {} ;
        value_type(const unsigned char * v, size_t l): bytes(v), size(l) {} ;
        [[nodiscard]] unsigned length() const
        {
            if(!size) return 0;
            return size - 1; // implied in the data is a null terminator
        }
        [[nodiscard]] const char * chars() const
        {
            return (const char*)bytes;
        }
        const unsigned char& operator[](unsigned i) const
        {
            // TODO: this is a hack fix because there's some BUG in the insert code
            // were assuming that the key has a magic 0 byte allocated after the last byte
            // however this is not so for data
            if (i < size)
            {
                return bytes[i];
            }
            throw std::out_of_range("index out of range");
        }

    };
}
#endif //VALUE_TYPE_H
