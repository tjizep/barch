//
// Created by teejip on 4/9/25.
//

#include "keys.h"
#include <cstdlib>
#include "conversion.h"

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
        if (ValkeyModule_ReplyWithStringBuffer(ctx, k, kl) == VALKEYMODULE_ERR) {
            return -1;
        }
    } else {
        abort();
    }
    return 0;
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
            return 10;
        } else {
            art::std_continue("[{integer}[", ik, "]");
            if (start) art::std_end();
            return 10;
        }
    } else if (key_len >= num32_key_size && (*enck == art::tshort || *enck == art::tfloat)) {
        sk = conversion::enc_bytes_to_int32(enck, key_len);
        if (*enck == art::tfloat) {
            memcpy(&fk, &sk, sizeof(sk));
            art::std_continue("{float}[", fk, "]");
            if (start) art::std_end();
            return num32_key_size;
        } else {
            art::std_continue("[{short}[", sk, "]");
            if (start) art::std_end();
            return num32_key_size;
        }
    } else if (key_len >= 1 && *enck == art::tstring) {
        k = (const char *) &enck[1];
        kl = key_len - 2;
        art::std_continue("{string}[", k, "][", kl, "]");
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
                    len = 10;
                    break;
                case art::tfloat:
                case art::tshort:
                    len = 6;
                    break;
                case art::tstring:
                    len = strnlen(ptr + 1, key_len - kl) + 2;
                    break;
                default:
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
