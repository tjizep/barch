//
// Created by teejip on 7/15/25.
//

#include "redis_parser.h"

#include <thread>
#include "sastam.h"
#include "statistics.h"
#include "asio/buffer.hpp"
#include <charconv>
#include <fast_float/fast_float.h>
namespace redis {
    template<typename T>
    bool to_t(const std::string_view& v, T &i) {

        auto ianswer = fast_float::from_chars(v.data(), v.data() + v.size(), i);
        return (ianswer.ec == std::errc() && ianswer.ptr == v.data() + v.size()) ;
    }
    template<typename T>
    bool to_ts(const std::string_view& sv, T &i) {

        auto result = std::from_chars(sv.data(), sv.data() + sv.size(), i);
        return !(result.ec == std::errc::invalid_argument) ;
    }
    /**
     * If there is a valid (CRLF terminated) item in buffer,
     * populate 'item' with it and return true.
     *
     * Returns false otherwise.
    */
    bool redis_parser::buffer_get_valid_item(art::value_type &item) {
        for (size_t i = buffer_start; i < buffer_size; i++) {
            item.size++;
            size_t tis = item.size;
            if(item.size >= 2 &&
               item[tis-2] == '\r' &&
               item[tis-1] == '\n') {
                buffer_start += tis;
                ++parameters_processed;
                return true;
            }
        }
        return false;
    }

    void redis_parser::add_data(const char * data, size_t len) {
        //buffer.append(data, len);
        if (buffer_size >= 2 && buffer_start >= buffer_size) {
            buffer_start = 0;
            full_buffer.clear();
            buffer_size = 0;
        }
        full_buffer.append(data, len);
        buffer_size+=len;
        max_buffer_size = std::max(buffer_size,max_buffer_size);
    }
    size_t redis_parser::remaining() const {
        return std::max(buffer_size, buffer_start) - buffer_start; //buffer.size();
    }
    std::string_view redis_parser::read_next_item() {
        //item.clear();
        auto item = art::value_type{full_buffer.data() + buffer_start, 0};
        if(!buffer_get_valid_item(item)) {
            if (item.size > redis_max_item_len) {
                throw_exception<std::domain_error>("item exceeds maximum length");
            }
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

    size_t redis_parser::get_max_buffer_size() const {
        return max_buffer_size;
    }

    const std::vector<std::string_view>& redis_parser::read_new_request(){
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
                    if (!to_t(sv, size)) {
                        throw_exception<std::domain_error>("invalid array size");
                    }
                    state = state_array_size;
                }
                    break;
                case state_array_size:
                    req.resize(size);
                    state = state_bstr;
                    item_nr = 0;
                    break;
                case state_bstr: {
                    if (item_nr >= size) {
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
                    if (!to_t(sv, bstr_size)) {
                        throw_exception<std::domain_error>("invalid array size");
                    }

                    if (bstr_size == -1) {
                        // null bulk string
                        static const char null[] = "NULL";
                        req[item_nr] = null;
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
                    // Read the bulk string
                    bstr_item = read_next_item();
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
                    *(char*)(bstr.data()+bstr.size()) = 0x00; // we need to do this because string views are not null terminated
                    req[item_nr++] = std::string_view{bstr.data(),bstr.length()};

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
