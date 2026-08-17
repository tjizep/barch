#include "sql.h"
#include "key_space.h"
#include "art/nodes.h"
#include "constants.h"
#include "conversion.h"
#include "keys.h"

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <fstream>

namespace barch {
namespace foreign {

std::string user_key(std::string_view k) {
    if (k.empty())
        return {};
    size_t start = 0;
    auto lead = static_cast<unsigned char>(k[0]);
    if (lead == art::tstring || lead == art::tinteger || lead == art::tdouble
        || lead == art::tshort || lead == art::tfloat
        || art::is_composite_lead(lead))
        start = 1;
    size_t end = k.size();
    while (end > start && k[end - 1] == 0)
        --end;
    return {k.data() + start, end - start};
}

static void trim(std::string& s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back())))
        s.pop_back();
    size_t i = 0;
    while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i])))
        ++i;
    if (i)
        s.erase(0, i);
}

std::string resolve_dsn(const key_space& ks, std::string& err) {
    std::string raw = ks.foreign_dsn;
    if (raw.size() >= 5 && raw.compare(0, 5, "file:") == 0) {
        std::ifstream in(raw.substr(5), std::ios::binary);
        if (!in) {
            err = "cannot read dsn file";
            return {};
        }
        std::string s((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        trim(s);
        if (s.empty()) {
            err = "dsn file empty";
            return {};
        }
        return s;
    }
    if (raw.size() >= 4 && raw.compare(0, 4, "env:") == 0) {
        const char* v = std::getenv(raw.substr(4).c_str());
        if (!v || !*v) {
            err = "dsn env not set";
            return {};
        }
        return v;
    }
    if (!raw.empty())
        return raw;
    if (ks.foreign_host.empty()) {
        err = "no dsn or host";
        return {};
    }
    std::string built = "host=" + ks.foreign_host;
    if (ks.foreign_port)
        built += " port=" + std::to_string(ks.foreign_port);
    if (!ks.foreign_user.empty())
        built += " user=" + ks.foreign_user;
    if (!ks.foreign_password.empty())
        built += " password=" + ks.foreign_password;
    if (!ks.foreign_database.empty())
        built += " dbname=" + ks.foreign_database;
    return built;
}

static bool is_type_lead(unsigned char lead) {
    return lead == art::tstring || lead == art::tinteger || lead == art::tdouble
        || lead == art::tshort || lead == art::tfloat
        || art::is_composite_lead(lead);
}

static size_t atom_str_len(const char* str, size_t len) {
    size_t i = 0;
    for (; i < len; ++i) {
        if (str[i] == 0 || str[i] == key_terminator)
            break;
    }
    return i;
}

static std::string decode_atom(std::string_view chunk) {
    if (chunk.empty())
        return {};
    auto lead = static_cast<unsigned char>(chunk[0]);
    if ((lead == art::tinteger || lead == art::tdouble) && chunk.size() >= numeric_key_size) {
        auto ik = conversion::enc_bytes_to_int(
            reinterpret_cast<const uint8_t*>(chunk.data()), numeric_key_size);
        if (lead == art::tdouble) {
            double dk = 0;
            memcpy(&dk, &ik, sizeof(ik));
            return numeric_to_text(dk);
        }
        return numeric_to_text(ik);
    }
    if ((lead == art::tshort || lead == art::tfloat) && chunk.size() >= num32_key_size) {
        auto sk = conversion::enc_bytes_to_int32(
            reinterpret_cast<const uint8_t*>(chunk.data()), num32_key_size);
        if (lead == art::tfloat) {
            float fk = 0;
            memcpy(&fk, &sk, sizeof(sk));
            return numeric_to_text(fk);
        }
        return numeric_to_text(sk);
    }
    if (lead == art::tstring) {
        auto n = atom_str_len(chunk.data() + 1, chunk.size() > 0 ? chunk.size() - 1 : 0);
        return {chunk.data() + 1, n};
    }
    return {chunk.begin(), chunk.end()};
}

std::vector<std::string> key_parts(std::string_view k) {
    if (k.empty())
        return {};
    auto lead = static_cast<unsigned char>(k[0]);
    if (!is_type_lead(lead))
        return {std::string(k)};
    if (!art::is_composite_lead(lead))
        return {decode_atom(k)};
    std::vector<std::string> out;
    size_t kl = 2;
    while (kl < k.size()) {
        const char* ptr = k.data() + kl;
        size_t left = k.size() - kl;
        unsigned len = 0;
        switch (static_cast<unsigned char>(*ptr)) {
            case art::tinteger:
            case art::tdouble:
                len = numeric_key_size;
                break;
            case art::tfloat:
            case art::tshort:
                len = num32_key_size;
                break;
            case art::tstring:
                if (left < 1)
                    return out;
                len = static_cast<unsigned>(atom_str_len(ptr + 1, left - 1) + 2);
                break;
            default:
                return out;
        }
        if (len == 0 || kl + len > k.size())
            break;
        out.push_back(decode_atom({ptr, len}));
        kl += len;
    }
    return out;
}

std::string key_encoded(std::string_view k) {
    if (k.empty())
        return {};
    if (!is_type_lead(static_cast<unsigned char>(k[0])))
        return std::string(k);
    return encoded_key_as_string(art::value_type{k.data(), k.size()});
}

static void emit_placeholder(std::string& sql, bool postgres, unsigned nth) {
    if (postgres) {
        sql += '$';
        sql += std::to_string(nth);
    } else {
        sql += '?';
    }
}

static bool ident_continue(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

bool compile_query(std::string_view in, bool postgres, compiled_query& out,
                   std::string_view space) {
    out.sql.clear();
    out.binds.clear();
    out.sql.reserve(in.size() + 8);
    for (size_t i = 0; i < in.size(); ++i) {
        char c = in[i];
        if (c == '\\' && i + 1 < in.size() && in[i + 1] == '$') {
            out.sql += '$';
            ++i;
            continue;
        }
        if (c == '?') {
            out.binds.push_back({bind_kind::user, 0});
            emit_placeholder(out.sql, postgres, static_cast<unsigned>(out.binds.size()));
            continue;
        }
        if (c == '$') {
            if (i + 1 < in.size() && in[i + 1] == '$') {
                out.binds.push_back({bind_kind::encoded, 0});
                emit_placeholder(out.sql, postgres, static_cast<unsigned>(out.binds.size()));
                ++i;
                continue;
            }
            if (i + 1 < in.size() && in[i + 1] == 'k'
                && (i + 2 >= in.size() || !ident_continue(in[i + 2]))) {
                out.sql.append(space.data(), space.size());
                ++i;
                continue;
            }
            if (i + 1 < in.size() && std::isdigit(static_cast<unsigned char>(in[i + 1]))) {
                unsigned n = 0;
                size_t j = i + 1;
                while (j < in.size() && std::isdigit(static_cast<unsigned char>(in[j]))) {
                    unsigned d = static_cast<unsigned>(in[j] - '0');
                    if (n > 99)
                        return false;
                    n = n * 10 + d;
                    ++j;
                }
                out.binds.push_back({bind_kind::part, n});
                emit_placeholder(out.sql, postgres, static_cast<unsigned>(out.binds.size()));
                i = j - 1;
                continue;
            }
        }
        out.sql += c;
    }
    return !out.binds.empty();
}

bool query_has_placeholder(std::string_view sql) {
    compiled_query q;
    return compile_query(sql, false, q, {});
}

bool query_has_one_placeholder(std::string_view sql) {
    return query_has_placeholder(sql);
}

bool bind_key(std::string_view internal, const compiled_query& q,
              std::vector<std::string>& values, std::string& err) {
    values.clear();
    values.reserve(q.binds.size());
    std::vector<std::string> parts;
    bool need_parts = false;
    for (auto& b : q.binds) {
        if (b.kind == bind_kind::part)
            need_parts = true;
    }
    if (need_parts)
        parts = key_parts(internal);
    for (auto& b : q.binds) {
        switch (b.kind) {
            case bind_kind::user:
                values.push_back(user_key(internal));
                break;
            case bind_kind::encoded:
                values.push_back(key_encoded(internal));
                break;
            case bind_kind::part:
                if (b.index >= parts.size()) {
                    err = "FOREIGN key has no component $" + std::to_string(b.index);
                    return false;
                }
                values.push_back(parts[b.index]);
                break;
        }
    }
    return true;
}

std::string postgres_placeholders(std::string_view sql) {
    compiled_query q;
    if (!compile_query(sql, true, q, {}))
        return std::string(sql);
    return q.sql;
}

}
}
