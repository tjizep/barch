//
// Created by teejip on 4/9/25.
//

#ifndef COMPOSITE_H
#define COMPOSITE_H
#include "conversion.h"
#include "value_type.h"

struct composite {
    typedef heap::small_vector<conversion::comparable_key> comparable_vector;
    typedef heap::small_vector<uint8_t, 64> byte_vector;

    template<typename VT>
    static art::value_type build_prefix(size_t end, const byte_vector &bytes, const VT &v) {
        size_t count = 0;
        size_t at = 0;
        for (const auto &i: v) {
            art::value_type k = i.get_value();
            count += k.size;
            ++at;
            if (at == end) break;
        }
        return {bytes.data(), count};
    }


    template<typename VT>
    static art::value_type build_key(byte_vector &result, const VT &v) {
        size_t count = 0;
        for (const auto &i: v) {
            art::value_type k = i.get_value();
            count += k.size;
        }
        result.resize(count);
        auto p = result.data();
        for (const auto &i: v) {
            art::value_type k = i.get_value();
            memcpy(p, k.bytes, k.size);
            p += k.size;
        }
        return {result.data(), result.size()};
    }

    comparable_vector comp{};
    byte_vector key_buffer{};

    composite() = default;

    composite(const composite &) = default;

    art::value_type create(std::initializer_list<conversion::comparable_key> from) {
        comp.clear();
        comp.push_back(art::ts_composite);
        for (const auto &i: from) {
            comp.push_back(i);
        }
        return create();
    }

    void push(const conversion::comparable_key &k) {
        comp.push_back(k);
    }

    void pop_back() {
        comp.pop_back();
    }

    void pop(size_t n) {
        size_t count = comp.size();
        comp.resize(count - std::min(count, n));
    }

    art::value_type create() {
        return build_key(key_buffer, comp);
    }

    [[nodiscard]] art::value_type end() const {
        return {key_buffer.data(), key_buffer.size()};
    }

    [[nodiscard]] art::value_type prefix(size_t length) const {
        return build_prefix(length, key_buffer, comp);
    }
};
#endif //COMPOSITE_H
