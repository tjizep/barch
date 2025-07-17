//
// Created by teejip on 7/15/25.
//

#include "redis_parser.h"

#include <thread>

#include "statistics.h"
#include "asio/buffer.hpp"

namespace redis {

    /**
     * If there is a valid (CRLF terminated) item in cache,
     * populate 'item' with it and return true.
     *
     * Returns false otherwise.
    */
    bool redis_parser::buffer_has_valid_item(std::string& item) {

        for (size_t i = 0; i < buffer.length(); i++) {

            item += buffer[i];

            if(item.length() >= 2 &&
               item[item.length()-2] == '\r' &&
               item[item.length()-1] == '\n') {

                // Remove the part read
                buffer = buffer.substr(i+1);
                return true;
            }
        }
        return false;
    }

    void redis_parser::add_data(const char * data, size_t len) {
        buffer.append(data, len);
    }
    size_t redis_parser::remaining() const {
        return buffer.size();
    }
    std::string redis_parser::read_next_item() {
        std::string item;
        if(!buffer_has_valid_item(item)) {
            if (item.length() > redis_max_item_len) {
                throw_exception<std::domain_error>("item exceeds maximum length");
            }
        }
        return item;
    }

    /**
     * size_item must look like:
     * "*<number-of-elements>\r\n"
    */
    bool redis_parser::validate_array_size(const std::string& size_item) {

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
        for (int i = 1; i <= len-3; i++) {
            if (size_item[i] < '0' || size_item[i] > '9') {
                return false;
            }
        }
        // valid
        return true;
    }

    /**
     * size_item must look like:
     * $<length>\r\n
    */
    bool redis_parser::validate_bstr_size(const std::string& size_item) {

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
        // Rest should be a number
        for (size_t i = 1; i <= len-3; i++) {
            if (size_item[i] < '0' || size_item[i] > '9') {
                return false;
            }
        }
        // valid
        return true;
    }

    /**
     * Returns true is bstr is terminated with CRLF
    */
    bool redis_parser::validate_crlf(const std::string& bstr) {

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
    const std::vector<std::string>& redis_parser::read_new_request(){
        while (state != state_end) {
            // Assumes each RESP request is an array of bulk strings
            switch (state) {
                case state_start: {
                    std::string arr_size_item = read_next_item();
                    if (arr_size_item.empty()) {
                        return empty;
                    }
                    if (!validate_array_size(arr_size_item)){
                        throw_exception<std::domain_error>("invalid array size");
                    }
                    size = std::stoi(arr_size_item.substr(1, arr_size_item.length()-3));
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
                        return req;
                    }
                    // Read <size> bulk strings
                    // Read size of the bulk string
                    std::string bstr_size_item = read_next_item();
                    if (bstr_size_item.empty()) {
                        return empty;
                    }
                    if (!validate_bstr_size(bstr_size_item)){
                        throw_exception<std::domain_error>("invalid bulk string size");
                    }
                    bstr_size = std::stoi(bstr_size_item.substr(1, bstr_size_item.length()-3));

                    if (bstr_size == -1) {
                        // null bulk string
                        req[item_nr] = "NULL";
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
                    std::string bstr_item = read_next_item();
                    if (bstr_item.empty()) {
                        return empty;
                    }
                    if (!validate_crlf(bstr_item)) {
                        throw_exception<std::domain_error>("Bulk string not terminated by CRLF");
                    }
                    std::string bstr = bstr_item.substr(0, bstr_item.length()-2);
                    if (bstr.length() != (size_t)bstr_size) {
                        throw_exception<std::domain_error>("Bulk string size does not match");
                    }

                    req[item_nr++] = bstr;

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
