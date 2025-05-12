//
// Created by linuxlite on 3/22/25.
//

#ifndef VALUE_TYPE_H
#define VALUE_TYPE_H
#include <cstddef>
#include <stdexcept>
#include <chrono>
#include "sastam.h"
#include "logical_address.h"

namespace art {
    struct value_type {
        const unsigned char *bytes;
        unsigned size;

        value_type() : bytes(nullptr), size(0) {
        }

        explicit value_type(nullptr_t): bytes(nullptr), size(0) {
        }

        explicit value_type(const logical_address &value): bytes((const uint8_t *) &value), size(sizeof(int64_t)) {
        }

        explicit value_type(const heap::buffer<uint8_t> &value): bytes(value.begin()), size(value.byte_size()) {
        }

        explicit value_type(const heap::vector<uint8_t> &value): bytes(value.data()), size(value.size()) {
        }

        explicit value_type(const heap::small_vector<uint8_t> &value): bytes(value.data()), size(value.size()) {
        }

        value_type(const char *v, size_t l): bytes((const unsigned char *) v), size(l) {
        }

        value_type(const unsigned char *v, unsigned l): bytes(v), size(l) {
        }

        value_type(const unsigned char *v, size_t l): bytes(v), size(l) {
        }

        [[nodiscard]] unsigned empty() const {
            return size == 0;
        }

        [[nodiscard]] unsigned length() const {
            if (!size) return 0;
            return size - 1; // implied in the data is a null terminator
        }

        [[nodiscard]] const char *chars() const {
            return (const char *) bytes;
        }

        [[nodiscard]] bool starts_with(value_type other) const {
            if (size < other.size) return false;
            return memcmp(bytes, other.bytes, other.size) == 0;
        }

        [[nodiscard]] int compare(value_type other) const {
            auto mins = std::min(size, other.size);
            int r = memcmp(bytes, other.bytes, mins);
            if (r == 0) {
                if (size < other.size) return -1;
                if (size > other.size) return 1;
                return 0;
            }
            return r;
        }

        bool operator <(const value_type &other) const {
            return compare(other) < 0;
        }

        bool operator >(const value_type &other) const {
            return compare(other) > 0;
        }

        bool operator !=(const value_type &other) const {
            return compare(other) != 0;
        }

        bool operator ==(const value_type &other) const {
            return compare(other) == 0;
        }

        bool operator <=(const value_type &other) const {
            return compare(other) <= 0;
        }

        bool operator >=(const value_type &other) const {
            return compare(other) >= 0;
        }

        [[nodiscard]] value_type sub(size_t start) const {
            if (start >= size) return value_type(nullptr);
            return {bytes + start, size - start};
        }

        [[nodiscard]] value_type sub(size_t start, size_t length) const {
            if (start + length >= size) return sub(start);
            return {bytes + start, length};
        }

        const unsigned char &operator[](unsigned i) const {
            // TODO: this is a hack fix because there's some BUG in the insert code
            // were assuming that the key has a magic 0 byte allocated after the last byte
            // however this is not so for data
            if (i < size) {
                return bytes[i];
            }
            abort_with("index out of range");
        }
    };
}
#endif //VALUE_TYPE_H
