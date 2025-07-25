//
// Created by teejip on 7/15/25.
//

#ifndef REDIS_PARSER_H
#define REDIS_PARSER_H
#include <iostream>
#include <vector>
#include "variable.h"
#include "ioutil.h"
#include "sastam.h"

namespace redis {
    enum {
        redis_max_item_len = 32000,
        redis_max_buffer_size = 4096
    };
    typedef std::function<size_t (char* buf,size_t bytes)> reader_t;
    static char CRLF[2] = {'\r','\n'};

    class redis_parser {
    private:
        bool buffer_has_valid_item(std::string& item);
        std::string read_next_item();
        bool validate_array_size(const std::string& size_item);
        bool validate_bstr_size(const std::string& size_item);
        bool validate_crlf(const std::string& bstr);
        heap::vector<std::string> empty{};
    public:
        redis_parser() = default;
        void init(char cs){ buffer += cs;};
        void add_data(const char * data, size_t len);
        size_t remaining() const ;
        const heap::vector<std::string>& read_new_request();
    private:
        int state = 0;
        int size = 0;
        heap::vector<std::string> req{};
        int item_nr = 0;
        int32_t bstr_size = 0;
        std::string item{};
        std::string buffer{};
    };

    inline bool is_bulk(const std::string& item) {
        if (item.empty()) { return false;}
        return item[0] == '$';
    }
    template<typename TS>
    inline void rwrite(TS& io, const std::string& v) {
        if (is_bulk(v)) {
            writep(io,'$');
            std::string size = std::to_string(v.size()-1);
            writep(io, size.data(), size.size());
            writep(io, CRLF);
            writep(io, v.data()+1, v.size()-1);
            writep(io, CRLF);
        }else {
            writep(io,'+');
            writep(io, v.data(), v.size());
            writep(io, CRLF);
        }

    }
    template<typename TS>
    inline void rwrite(TS& io, const error& v) {
        writep(io,'-');
        writep(io, v.name.data(), v.name.size());
        writep(io, CRLF);
    }
    template<typename TS>
    inline void rwrite(TS& io, bool v) {
        writep(io,':');
        writep(io, v ? '1':'0');
        writep(io, CRLF);
    }

    template<typename TS>
    inline void rwrite(TS& io, int64_t i) {
        std::string v = std::to_string(i);
        writep(io,':');
        writep(io, v.data(), v.size());
        writep(io, CRLF);
    }
    template<typename TS>
    inline void rwrite(TS& io, double d) {
        std::string v = std::to_string(d);
        rwrite(io, v.c_str());
    }

    template<typename TS>
    inline void rwrite(TS& io, nullptr_t) {
        writep(io,'$');
        writep(io, "-1");
        writep(io, CRLF);
    }

    template<typename TS>
    inline void rwrite(TS& io, const Variable& v) {
        std::string buffer;
        switch (v.index()) {
            case var_bool:
                rwrite(io, *std::get_if<bool>(&v));
                break;
            case var_int64:
                rwrite(io, *std::get_if<int64_t>(&v));
                break;
            case var_double:
                rwrite(io, *std::get_if<double>(&v));
                break;
            case var_string:
                rwrite(io, *std::get_if<std::string>(&v));
                break;
            case var_null:
                rwrite(io, nullptr);
                break;
            case var_error:
                rwrite(io, *std::get_if<error>(&v));
                break;
            default:
                break;
        }
    }

    template<typename TS>
    inline void rwrite(TS& io, const heap::vector<Variable>& v) {
        if (v.empty()) {
            rwrite(io,nullptr);
            return;
        }
        if (v.size() == 1) {
            rwrite(io, v[0]);
            return;
        }
        writep(io, '*');
        std::string size = std::to_string(v.size());
        writep(io, size.data(), size.size());
        writep(io, CRLF);
        for (const auto &item: v) {
            rwrite(io, item);
        }
    }
}
#endif //REDIS_PARSER_H
