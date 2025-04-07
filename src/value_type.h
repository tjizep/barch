//
// Created by linuxlite on 3/22/25.
//

#ifndef VALUE_TYPE_H
#define VALUE_TYPE_H
#include <cstddef>
#include <stdexcept>
#include <chrono>
#include "sastam.h"

namespace art
{

    struct value_type
    {
        const unsigned char* bytes;
        unsigned size;

        explicit value_type(const heap::buffer<uint8_t>& value): bytes(value.begin()), size(value.byte_size())
        {
        }

        explicit value_type(const heap::vector<uint8_t>& value): bytes(value.data()), size(value.size())
        {
        }

        value_type(const char* v, unsigned l): bytes((const unsigned char*)v), size(l)
        {
        }

        value_type(const unsigned char* v, unsigned l): bytes(v), size(l)
        {
        }

        value_type(const unsigned char* v, size_t l): bytes(v), size(l)
        {
        }

        [[nodiscard]] unsigned length() const
        {
            if (!size) return 0;
            return size - 1; // implied in the data is a null terminator
        }

        [[nodiscard]] const char* chars() const
        {
            return (const char*)bytes;
        }
        [[nodiscard]] bool starts_with(value_type other) const
        {
            if (size < other.size) return false;
            return memcmp(bytes, other.bytes, other.size) == 0;
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
