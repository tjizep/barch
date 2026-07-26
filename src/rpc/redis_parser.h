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
    typedef std::string string_param_t;
    enum {
        redis_max_item_len = 6400000
    };
    typedef std::function<size_t (char* buf,size_t bytes)> reader_t;
    static char CRLF[2] = {'\r','\n'};

    class redis_parser {
    private:
        bool buffer_get_valid_item(art::value_type &item, ptrdiff_t hint = 0);
        std::string_view read_next_item();
        std::string_view read_next_item(ptrdiff_t hint) ;
        bool validate_array_size(const std::string_view& size_item);
        bool validate_bstr_size(const std::string_view& size_item);
        bool validate_crlf(const std::string_view& bstr);
        std::vector<string_param_t> empty{};
    public:
        redis_parser() = default;
        void init(char cs){
            full_buffer=cs;
            buffer_size=1;
        };
        void add_data(const char * data, size_t len);
        [[nodiscard]] size_t remaining() const ;
        const std::vector<string_param_t>& read_new_request();
        size_t get_max_buffer_size() const;
    private:
        int state = 0;
        int size = 0;
        std::vector<string_param_t> req{};
        int item_nr = 0;
        int32_t bstr_size = 0;
        size_t buffer_start = 0l;
        size_t buffer_size = 0l;
        size_t max_buffer_size = 0l;
        size_t parameters_processed = 0l;
        size_t messages_processed = 0l;
        std::string full_buffer{};
        std::string_view arr_size_item{};
        std::string_view bstr_item{};
        std::string_view bstr_size_item{};
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
        writep(io, v.what(), v.size());
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
    inline void rwrite(TS& io, uint64_t i) {
        std::string v = std::to_string(i);
        writep(io,':');
        writep(io, v.data(), v.size());
        writep(io, CRLF);
    }
    template<typename TS>
    inline void rwrite(TS& io, double d) {
        // this used to hand v.c_str() back to rwrite, where const char* binds to the
        // bool overload ahead of std::string, so every double went out as ':1'
        std::string v = fmt::format("{}", d);
        writep(io, '$');
        std::string size = std::to_string(v.size());
        writep(io, size.data(), size.size());
        writep(io, CRLF);
        writep(io, v.data(), v.size());
        writep(io, CRLF);
    }

    template<typename TS>
    inline void rwrite(TS& io, nullptr_t) {
        writep(io,'$');
        writep(io, "-1");
        writep(io, CRLF);
    }

    /**
     * RESP3 is a superset of RESP2: '+', '-', ':', '$' and '*' mean the same in both, so
     * only the shapes that gained a type of their own are switched on the protocol.
     * A connection that never sent HELLO 3 stays on 2 and sees exactly what it always did.
     */
    inline constexpr bool resp3(int protocol) {
        return protocol >= 3;
    }
    // a header such as '*3', '%2' or '~4'
    template<typename TS>
    inline void rwrite_header(TS& io, char type, size_t count) {
        writep(io, type);
        std::string size = std::to_string(count);
        writep(io, size.data(), size.size());
        writep(io, CRLF);
    }
    template<typename TS>
    inline void rwrite_null(TS& io, int protocol) {
        if (resp3(protocol)) {
            writep(io, '_');
            writep(io, CRLF);
            return;
        }
        rwrite(io, nullptr);
    }
    template<typename TS>
    inline void rwrite_bool(TS& io, bool value, int protocol) {
        if (resp3(protocol)) {
            writep(io, '#');
            writep(io, value ? 't' : 'f');
            writep(io, CRLF);
            return;
        }
        rwrite(io, value);
    }
    template<typename TS>
    inline void rwrite_double(TS& io, double d, int protocol) {
        std::string v = fmt::format("{}", d);
        if (resp3(protocol)) {
            writep(io, ',');
            writep(io, v.data(), v.size());
            writep(io, CRLF);
            return;
        }
        // RESP2 has no double, so it travels as a bulk string the way redis sends it
        writep(io, '$');
        std::string size = std::to_string(v.size());
        writep(io, size.data(), size.size());
        writep(io, CRLF);
        writep(io, v.data(), v.size());
        writep(io, CRLF);
    }
    template<typename TS>
    inline void rwrite_verbatim(TS& io, const verbatim_t& v, int protocol) {
        if (resp3(protocol)) {
            // '=' carries a three letter format, a colon, then the text
            writep(io, '=');
            std::string size = std::to_string(v.text.size() + 4);
            writep(io, size.data(), size.size());
            writep(io, CRLF);
            std::string format = v.format.substr(0, 3);
            while (format.size() < 3) format += ' ';
            writep(io, format.data(), format.size());
            writep(io, ':');
            writep(io, v.text.data(), v.text.size());
            writep(io, CRLF);
            return;
        }
        writep(io, '$');
        std::string size = std::to_string(v.text.size());
        writep(io, size.data(), size.size());
        writep(io, CRLF);
        writep(io, v.text.data(), v.text.size());
        writep(io, CRLF);
    }

    template<typename TS>
    inline void rwrite(TS& io, const Variable& v, int protocol = 2) {
        switch (v.index()) {
            case var_bool:
                rwrite_bool(io, *std::get_if<bool>(&v), protocol);
                break;
            case var_int64:
                rwrite(io, *std::get_if<int64_t>(&v));
                break;
            case var_uint64:
                rwrite(io, *std::get_if<uint64_t>(&v));
                break;
            case var_double:
                rwrite_double(io, *std::get_if<double>(&v), protocol);
                break;
            case var_string:
                rwrite(io, *std::get_if<std::string>(&v));
                break;
            case var_null:
                rwrite_null(io, protocol);
                break;
            case var_array:
            case var_map:
            case var_set: {
                const auto &a = v.elements();
                // a map counts pairs, not elements, and both fall back to a flat array
                // on RESP2 - which is exactly how RESP2 has always carried them
                if (resp3(protocol) && v.index() == var_map) {
                    rwrite_header(io, '%', a.size() / 2);
                } else if (resp3(protocol) && v.index() == var_set) {
                    rwrite_header(io, '~', a.size());
                } else {
                    rwrite_header(io, '*', a.size());
                }
                for (const auto& el: a) {
                    rwrite(io, (const Variable&) el, protocol);
                }
            }
                break;
            case var_verbatim:
                rwrite_verbatim(io, *std::get_if<verbatim_t>(&v), protocol);
                break;
            case var_error:
                rwrite(io, *std::get_if<error>(&v));
                break;
            default:
                break;
        }
    }

    template<typename TS>
    inline void rwrite(TS& io, const heap::vector<Variable>& v, int protocol = 2) {
        if (v.empty()) {
            rwrite_null(io, protocol);
            return;
        }
        if (v.size() == 1) {
            rwrite(io, v[0], protocol);
            return;
        }
        rwrite_header(io, '*', v.size());
        for (const auto &item: v) {
            rwrite(io, item, protocol);
        }
    }
}
#endif //REDIS_PARSER_H
