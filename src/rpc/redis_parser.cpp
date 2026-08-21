//
// Created by teejip on 7/15/25.
//

#include "redis_parser.h"

#include <thread>
#include "sastam.h"
#include "statistics.h"
#include "asio/buffer.hpp"
#include <cstdint>
#include <limits>
namespace redis {
    enum {
        state_start = 0,
        state_array_size,
        state_bstr_size,
        state_bstr,
        state_crlf,
        state_end,
        state_error,
        state_max
    };
    // RESP array and bulk lengths are small integers. fast_float is for
    // real numbers; a digit loop is enough here and showed up at ~2% self
    static bool parse_resp_int32(std::string_view v, int32_t& out) {
        if (v.empty()) return false;
        const char* p = v.data();
        const char* e = p + v.size();
        bool neg = false;
        if (*p == '-') {
            neg = true;
            ++p;
            if (p == e) return false;
        }
        uint32_t acc = 0;
        for (; p != e; ++p) {
            unsigned d = (unsigned char)*p - '0';
            if (d > 9) return false;
            if (acc > (uint32_t)std::numeric_limits<int32_t>::max() / 10)
                return false;
            acc = acc * 10 + d;
        }
        if (neg) {
            if (acc > (uint32_t)std::numeric_limits<int32_t>::max() + 1)
                return false;
            out = acc == (uint32_t)std::numeric_limits<int32_t>::max() + 1
                      ? std::numeric_limits<int32_t>::min()
                      : -(int32_t)acc;
        } else {
            if (acc > (uint32_t)std::numeric_limits<int32_t>::max())
                return false;
            out = (int32_t)acc;
        }
        return true;
    }
    static constexpr uint32_t k_null_bulk = std::numeric_limits<uint32_t>::max();
    /**
     * If there is a valid (CRLF terminated) item in buffer,
     * populate 'item' with it and return true.
     *
     * Returns false otherwise.
    */
    bool redis_parser::buffer_get_valid_item(art::value_type &item, ptrdiff_t hint) {
        if (buffer_size - buffer_start < 2) {
            item.size = buffer_size - buffer_start;
            return false;
        }
        ptrdiff_t rem = (ptrdiff_t)(buffer_size - buffer_start);
        ptrdiff_t end = rem + 1;
        if (hint >= 0) {
            // payload length, including 0 for `$0\r\n\r\n`. wait for the
            // terminator at that offset; an earlier CRLF is data.
            if (hint + 1 >= rem) {
                item.size = rem;
                return false;
            }
            if (item.bytes[hint] == '\r' &&
                item.bytes[hint + 1] == '\n') {
                item.size = hint + 2;
                buffer_start += hint + 2;
                ++parameters_processed;
                return true;
            }
        } else if (rem >= 4) {
            // *2\r\n, $3\r\n, $16\r\n: CRLF sits in the first few bytes.
            ptrdiff_t maxc = rem < 14 ? rem - 1 : 13;
            for (ptrdiff_t i = 2; i < maxc; ++i) {
                if (item.bytes[i] == '\r' && item.bytes[i + 1] == '\n') {
                    item.size = i + 2;
                    buffer_start += i + 2;
                    ++parameters_processed;
                    return true;
                }
            }
        }
        item.size = 2;

        for (ptrdiff_t i = 2; i < end; i++) {
            const auto* d = (const uint8_t*) memchr(&item.bytes[i-2],'\r',end);
            if (d == nullptr) break;
            i += (d - &item.bytes[i-2]);
            if (item.bytes[i-2] == '\r' &&
                item.bytes[i-1] == '\n') {
                item.size = i;
                buffer_start += i;
                ++parameters_processed;
                return true;
            }
        }

        return false;
    }

    void redis_parser::add_data(const char * data, size_t len) {
        // only drop a consumed buffer between requests. a partial parse
        // holds offsets into full_buffer, so clearing here used to make
        // params[0] a slice of the next packet
        if (state == state_start && item_nr == 0 && buffer_start >= buffer_size) {
            buffer_start = 0;
            full_buffer.clear();
            buffer_size = 0;
        }
        full_buffer.append(data, len);
        buffer_size += len;
        max_buffer_size = std::max(buffer_size,max_buffer_size);
    }
    size_t redis_parser::remaining() const {
        return std::max(buffer_size, buffer_start) - buffer_start; //buffer.size();
    }
    std::string_view redis_parser::read_next_item() {
        auto item = art::value_type{full_buffer.data() + buffer_start, 0};
        if(!buffer_get_valid_item(item)) {
            if (item.size > redis_max_item_len) {
                throw_exception<std::domain_error>("item exceeds maximum length");
            }
            return std::string_view{};
        }
        return std::string_view{item.chars(),item.size};
    }
    std::string_view redis_parser::read_next_item(ptrdiff_t hint) {
        auto item = art::value_type{full_buffer.data() + buffer_start, 0};
        if(!buffer_get_valid_item(item, hint)) {
            if (item.size > redis_max_item_len) {
                throw_exception<std::domain_error>("item exceeds maximum length");
            }
            return std::string_view{};
        }
        return std::string_view{item.chars(),item.size};
    }

    /**
     * size_item must look like:
     * "*<number-of-elements>\r\n"
    */
    bool redis_parser::validate_array_size(const std::string_view& size_item) {

        int len = size_item.length();
        // Must be atleast 4 characters
        if (len < 4) {
            return false;
        }
        // Must begin with *
        if (size_item[0] != '*') {
            return false;
        }
        // Must end with \r\n
        if (size_item[len-1] != '\n' || size_item[len-2] != '\r') {
            return false;
        }
        // Rest should be a number

        // valid
        return true;
    }

    /**
     * size_item must look like:
     * $<length>\r\n
    */
    bool redis_parser::validate_bstr_size(const std::string_view& size_item) {

        size_t len = size_item.length();
        // Must be atleast 4 characters
        if (len < 4) {
            return false;
        }
        // Must begin with $
        if (size_item[0] != '$') {
            return false;
        }
        // Must end with \r\n
        if (size_item[len-1] != '\n' || size_item[len-2] != '\r') {
            return false;
        }
        // Rest should be a number - is checked by conversion
        // valid
        return true;
    }

    /**
     * Returns true is bstr is terminated with CRLF
    */
    bool redis_parser::validate_crlf(const std::string_view& bstr) {

        size_t len = bstr.length();
        if (len < 2) {
            return false;
        }
        return bstr[len-2] == '\r' && bstr[len-1] == '\n';

    }

    size_t redis_parser::get_max_buffer_size() const {
        return max_buffer_size;
    }

    const std::vector<string_param_t>& redis_parser::read_new_request(){
        while (state != state_end) {
            // Assumes each RESP request is an array of bulk strings
            switch (state) {
                case state_start: {
                    arr_size_item = read_next_item();
                    if (arr_size_item.empty()) {
                        return empty;
                    }
                    if (!validate_array_size(arr_size_item)){
                        throw_exception<std::domain_error>("invalid array size");
                    }
                    auto sv = std::string_view{arr_size_item.data()+1, arr_size_item.length()-3};
                    int32_t n = 0;
                    if (!parse_resp_int32(sv, n) || n < 0) {
                        throw_exception<std::domain_error>("invalid array size");
                    }
                    size = n;
                    state = state_array_size;
                }
                    break;
                case state_array_size:
                    spans.resize((size_t)size);
                    state = state_bstr;
                    item_nr = 0;
                    break;
                case state_bstr: {
                    if (item_nr >= size) {
                        req.resize((size_t)size);
                        const char* base = full_buffer.data();
                        for (int i = 0; i < size; ++i) {
                            auto [off, len] = spans[(size_t)i];
                            if (off == k_null_bulk)
                                req[(size_t)i] = std::string_view{"NULL", 4};
                            else
                                req[(size_t)i] = std::string_view{base + off, len};
                        }
                        state = state_start;
                        size = 0;
                        item_nr = 0;
                        ++messages_processed;
                        return req;
                    }
                    // Read <size> bulk strings
                    // Read size of the bulk string
                    bstr_size_item = read_next_item();
                    if (bstr_size_item.empty()) {
                        return empty;
                    }
                    if (!validate_bstr_size(bstr_size_item)){
                        throw_exception<std::domain_error>("invalid bulk string size");
                    }

                    auto sv = std::string_view{bstr_size_item.data()+1, bstr_size_item.length()-3};
                    if (!parse_resp_int32(sv, bstr_size)) {
                        throw_exception<std::domain_error>("invalid array size");
                    }

                    if (bstr_size == -1) {
                        spans[(size_t)item_nr] = {k_null_bulk, 4};
                        ++item_nr;
                        continue;
                    }
                    if (bstr_size < -1) {
                        throw_exception<std::domain_error>("Bulk string size < -1");
                    }
                    state = state_bstr_size;
                }
                    break;
                case state_bstr_size: {
                    bstr_item = read_next_item(bstr_size);
                    if (bstr_item.empty()) {
                        return empty;
                    }
                    if (!validate_crlf(bstr_item)) {
                        throw_exception<std::domain_error>("Bulk string not terminated by CRLF");
                    }
                    std::string_view bstr = std::string_view{bstr_item.data(),bstr_item.length()-2};
                    if (bstr.length() != (size_t)bstr_size) {
                        throw_exception<std::domain_error>("Bulk string size does not match");
                    }
                    *(char*)(bstr.data()+bstr.size()) = 0x00;
                    auto off = (uint32_t)(bstr.data() - full_buffer.data());
                    spans[(size_t)item_nr] = {off, (uint32_t)bstr.length()};
                    ++item_nr;

                    state = state_bstr;
                }
                    break;
                default:
                    throw_exception<std::domain_error>("Bulk string not terminated by CRLF");
            }
        }
        return empty;
    }
}
