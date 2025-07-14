//
// Created by teejip on 4/9/25.
//

#include "keys.h"
#include <cstdlib>
#include "conversion.h"
static size_t encoded_str_len(const char* str, size_t len) {
    size_t i = 0;
    for (; i < len; ++i) {
        if (str[i] == 0 || str[i] == key_terminator)
            break;
    }
    return i;
}

unsigned art::key_type_size(value_type key) {
    switch (key.bytes[0]) {
        case tdouble:
            return numeric_key_size;
        case tfloat:
            return num32_key_size;
        case tinteger:
            return numeric_key_size;
        case tshort:
            return num32_key_size;
        case tstring:
            return strnlen(key.chars() + 1, key.length()) + 2;
        default:
            abort_with("unsupported key type");
    }
}

int key_ok(const char *k, size_t klen) {
    if (k == nullptr)
        return -1;

    if (klen == 0)
        return -1;

    return 0;
}
int key_ok(art::value_type v) {
    return key_ok(v.chars(), v.size);
}

int key_check(ValkeyModuleCtx *ctx, const char *k, size_t klen) {
    if (k == nullptr)
        return ValkeyModule_ReplyWithError(ctx, "No null keys");

    if (klen == 0)
        return ValkeyModule_ReplyWithError(ctx, "No empty keys");


    return ValkeyModule_ReplyWithError(ctx, "Unspecified key error");
}


int reply_encoded_key(ValkeyModuleCtx *ctx, art::value_type key) {
    double dk;
    float fk;
    int64_t ik;
    int32_t sk;
    const char *k;
    size_t kl;
    const unsigned char *enck = key.bytes;
    unsigned key_len = key.size;
    // TODO: integers sometimes go in here as one longer than they should be
    // we make the test a little more slack
    if (key_len >= numeric_key_size && (*enck == art::tinteger || *enck == art::tdouble)) {
        ik = conversion::enc_bytes_to_int(enck, key_len);
        if (*enck == art::tdouble) {
            memcpy(&dk, &ik, sizeof(ik));
            if (ValkeyModule_ReplyWithDouble(ctx, dk) == VALKEYMODULE_ERR) {
                return -1;
            }
        } else {
            if (ValkeyModule_ReplyWithLongLong(ctx, ik) == VALKEYMODULE_ERR) {
                return -1;
            }
        }
    } else if (key_len >= num32_key_size && (*enck == art::tshort || *enck == art::tfloat)) {
        sk = conversion::enc_bytes_to_int32(enck, key_len);
        if (*enck == art::tfloat) {
            memcpy(&fk, &sk, sizeof(sk));
            if (ValkeyModule_ReplyWithDouble(ctx, fk) == VALKEYMODULE_ERR) {
                return -1;
            }
        } else {
            if (ValkeyModule_ReplyWithLongLong(ctx, sk) == VALKEYMODULE_ERR) {
                return -1;
            }
        }
    } else if (key_len >= 1 && *enck == art::tstring) {
        k = (const char *) &enck[1];
        kl = key_len - 2;
        if (ValkeyModule_ReplyWithStringBuffer(ctx, k, encoded_str_len(k, kl)) == VALKEYMODULE_ERR) {
            return -1;
        }
    } else if (key_len >= 1 && (*enck == art::tcomposite)) {
        return reply_encoded_key(ctx, key.sub(2));
    } else {
        abort();
    }
    return 0;
}

/**
 * function just returns the first key in a composite
 * @param key
 * @return
 */
Variable encoded_key_as_variant(art::value_type key) {
    double dk;
    float fk;
    int64_t ik;
    int32_t sk;
    const char *k;
    //size_t kl;
    const unsigned char *enck = key.bytes;
    unsigned key_len = key.size;
    // TODO: integers sometimes go in here as one longer than they should be
    // we make the test a little more slack
    if (key_len >= numeric_key_size && (*enck == art::tinteger || *enck == art::tdouble)) {
        ik = conversion::enc_bytes_to_int(enck, key_len);
        if (*enck == art::tdouble) {
            memcpy(&dk, &ik, sizeof(ik));
            return dk;
        } else {
            return ik;
        }
    } else if (key_len >= num32_key_size && (*enck == art::tshort || *enck == art::tfloat)) {
        sk = conversion::enc_bytes_to_int32(enck, key_len);
        if (*enck == art::tfloat) {
            memcpy(&fk, &sk, sizeof(sk));
            return fk;
        } else {
            return sk;
        }
    } else if (key_len >= 1 && *enck == art::tstring) {
        k = (const char *) &enck[1];
        // kl = key_len - 2;
        std::string s;
        s.insert(s.end(), k, k + encoded_str_len(k, key_len - 2));
        return s;

    } else if (key_len >= 1 && (*enck == art::tcomposite)) {
        return encoded_key_as_variant(key.sub(2));
    } else {
        abort();
    }
    return "";
}
std::string encoded_key_as_string(art::value_type key) {
    Variable v = encoded_key_as_variant(key);
    switch (v.index()) {
        case 0:
            return std::to_string(std::get<bool>(v));
        case 1:
            return std::to_string(std::get<int64_t>(v));
        case 2:
            return std::to_string(std::get<double>(v));
        case 3:
            return std::get<std::string>(v);
        case 4:
            return {};
        default:
            abort_with("invalid type");
    }
    return "";
}
unsigned log_encoded_key(art::value_type key, bool start) {
    double dk;
    float fk;
    int64_t ik;
    int64_t sk;
    const char *k;
    size_t kl;
    const unsigned char *enck = key.bytes;
    unsigned key_len = key.size;
    // TODO: integers sometimes go in here as one longer than they should be
    // we make the test a little more slack
    if (start) art::std_start();
    if (key_len >= numeric_key_size && (*enck == art::tinteger || *enck == art::tdouble)) {
        ik = conversion::enc_bytes_to_int(enck, key_len);
        if (*enck == art::tdouble) {
            memcpy(&dk, &ik, sizeof(ik));
            art::std_continue("{double}[", dk, "]");
            if (start) art::std_end();
            return numeric_key_size;
        } else {
            art::std_continue("{integer}[", ik, "]");
            if (start) art::std_end();
            return numeric_key_size;
        }
    } else if (key_len >= num32_key_size && (*enck == art::tshort || *enck == art::tfloat)) {
        sk = conversion::enc_bytes_to_int32(enck, key_len);
        if (*enck == art::tfloat) {
            memcpy(&fk, &sk, sizeof(sk));
            art::std_continue("{float}[", fk, "]");
            if (start) art::std_end();
            return num32_key_size;
        } else {
            art::std_continue("{short}[", sk, "]");
            if (start) art::std_end();
            return num32_key_size;
        }
    } else if (key_len >= 1 && *enck == art::tstring) {
        k = (const char *) &enck[1];
        kl = key_len - 2;
        std::string s;
        s.insert(s.end(), k, k + kl);
        art::std_continue("{string}[", s, "][", kl, "]");
        if (start) art::std_end();
        return 2 + kl;
    } else if (key_len > 1 && *enck == art::tcomposite) {
        art::std_continue("<", 2, ">");
        art::std_continue("{composite}[");
        unsigned kl = 2;
        const char *ptr = (const char *) &enck[2];
        while (kl < key_len) {
            unsigned len = 0;

            switch (*ptr) {
                case art::tinteger:
                case art::tdouble:
                    len = numeric_key_size;
                    break;
                case art::tfloat:
                case art::tshort:
                    len = num32_key_size;
                    break;
                case art::tstring: {
                    len = encoded_str_len(ptr + 1,key_len - kl) + 2;
                }
                    break;
                default:
                    art::std_continue("<key or data error>");
                    if (start) art::std_end();
                    return 0;
            }
            art::std_continue("<", len, ">");
            ptr += log_encoded_key({ptr, len}, false);
            kl += len;
        }
        art::std_continue("] <", kl, ">");
        if (start) art::std_end();
        return key_len;
    }
    if (start) art::std_end();
    return 0;
}
