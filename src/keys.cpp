//
// Created by teejip on 4/9/25.
//

#include "keys.h"
#include "caller.h"
#include "composite.h"
#include <cerrno>
#include <cmath>
#include <limits>
// std_start/std_continue/std_end - the streaming form, which lzr_log does not have
#include "logger.h"
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

int key_ok(const char *k, size_t klen) {
    if (k == nullptr)
        return -1;

    if (klen == 0)
        return -1;

    //if (strnlen(k, klen) < klen) {
    //    return -1;
    //}
    return 0;
}
bool fits_in_leaf(size_t key_bytes, size_t value_bytes) {
    return key_bytes + value_bytes <= (size_t) maximum_allocation_size;
}

const char *too_large_message() {
    return "string exceeds maximum allowed size";
}

int key_ok(art::value_type v) {
    return key_ok(v.chars(), v.size);
}

bool parse_block_timeout(caller& cc, art::value_type text, uint64_t& time_out) {
    // strtod rather than the strict reader: redis parses the timeout with the C
    // library, which takes hex and exponent forms, and then judges the value. A
    // caller writing 0x7FFFFFFFFFFFFF should be told the number is too big, not
    // that it is not a number
    std::string t(text.chars(), text.size);
    char *tail = nullptr;
    errno = 0;
    double secs = std::strtod(t.c_str(), &tail);
    if (t.empty() || tail != t.c_str() + t.size() || std::isnan(secs)) {
        cc.push_error("timeout is not a float or out of range");
        return false;
    }
    if (secs < 0) {
        cc.push_error("timeout is negative");
        return false;
    }
    // the wait is kept in milliseconds, so a timeout that cannot be one is not a
    // very long wait, it is an unreadable number
    if (errno == ERANGE || std::isinf(secs)
        || !(secs * 1000.0 < (double) std::numeric_limits<int64_t>::max())) {
        cc.push_error("timeout is out of range");
        return false;
    }
    time_out = (uint64_t) (secs * 1000.0);
    return true;
}

int key_check(ValkeyModuleCtx *ctx, const char *k, size_t klen) {
    if (k == nullptr)
        return ValkeyModule_ReplyWithError(ctx, "No null keys");

    if (klen == 0)
        return ValkeyModule_ReplyWithError(ctx, "No empty keys");
    //if (strnlen(k, klen) < klen) {
    //    return ValkeyModule_ReplyWithError(ctx, "No keys with embedded nulls");
    //}

    return ValkeyModule_ReplyWithError(ctx, "Unspecified key error");
}


int reply_encoded_key(ValkeyModuleCtx *ctx, art::value_type key) {
    // TODO: should probably use the existing function in conversion.cpp

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
        ik = conversion::enc_bytes_to_int(enck, numeric_key_size);
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
        sk = conversion::enc_bytes_to_int32(enck, num32_key_size);
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
    } else if (key_len >= 1 && art::is_composite_lead(*enck)) {

        return reply_encoded_key(ctx, key.sub(2));
    } else {
        abort();
    }
    return 0;
}
int reply_variable(ValkeyModuleCtx *ctx, const Variable var) {
    switch (var.index()) {
        case var_bool:
            return ValkeyModule_ReplyWithBool(ctx, std::get<bool>(var));
        case var_int64:
            return ValkeyModule_ReplyWithLongLong(ctx, std::get<int64_t>(var));
        case var_uint64:
            return ValkeyModule_ReplyWithLongLong(ctx, std::get<uint64_t>(var));
        case var_double:
            return ValkeyModule_ReplyWithDouble(ctx, std::get<double>(var));
        case var_string:
            return ValkeyModule_ReplyWithStringBuffer(ctx, std::get<std::string>(var).c_str(), std::get<std::string>(var).size());
        case var_null:
            return ValkeyModule_ReplyWithNull(ctx);
        case var_error:
            return ValkeyModule_ReplyWithError(ctx, std::get<error>(var).what());
        default:
            return ValkeyModule_ReplyWithNull(ctx);
    }
    return 0;
}

/**
 * function just returns the first key in a composite
 * @param key
 * @return
 */
Variable encoded_key_as_variant(art::value_type key, char sep) {
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
        ik = conversion::enc_bytes_to_int(enck, numeric_key_size);
        if (*enck == art::tdouble) {
            memcpy(&dk, &ik, sizeof(ik));
            return dk;
        } else {
            return ik;
        }
    } else if (key_len >= num32_key_size && (*enck == art::tshort || *enck == art::tfloat)) {
        sk = conversion::enc_bytes_to_int32(enck, num32_key_size);
        if (*enck == art::tfloat) {
            memcpy(&fk, &sk, sizeof(sk));
            return fk;
        } else {
            return sk;
        }
    } else if (key_len >= 2 && *enck == art::tstring) {
        k = (const char *) &enck[1];
        // kl = key_len - 2;
        std::string s = "$";
        s.insert(s.end(), k, k + encoded_str_len(k, key_len - 2));
        return s;

    } else if (key_len >= 1 && art::is_composite_lead(*enck)) {
        unsigned kl = 2;
        std::string r = "$";
        size_t cnt = 0;
        while (kl < key_len) {
            const unsigned char* ptr = enck + kl;
            unsigned left = key_len - kl;
            unsigned len = 0;
            std::string part;
            switch (*ptr) {
                case art::tinteger:
                case art::tdouble:
                    len = numeric_key_size;
                    if (len > left)
                        return r;
                    part = encoded_key_as_variant({ptr, len}, sep).s();
                    break;
                case art::tfloat:
                case art::tshort:
                    len = num32_key_size;
                    if (len > left)
                        return r;
                    part = encoded_key_as_variant({ptr, len}, sep).s();
                    break;
                case art::tstring: {
                    size_t n = encoded_str_len(reinterpret_cast<const char*>(ptr + 1),
                                              left > 0 ? left - 1 : 0);
                    len = static_cast<unsigned>(n + 2);
                    if (len > left)
                        return r;
                    part.assign(reinterpret_cast<const char*>(ptr + 1), n);
                    break;
                }
                default:
                    return r;
            }
            if (cnt > 0)
                r += sep;
            r += part;
            kl += len;
            ++cnt;
        }
        return r;
    } else {
        return std::string{};
    }
    return "";
}
/**
 * How much of a container's key names the container.
 *
 * A list, hash or ordered set stores one key per entry, all of them beginning with the
 * kind's lead byte followed by the name. Slicing a key at the end of that first component
 * leaves something that still decodes as a composite, but decodes to the name alone - so
 * both the glob matcher and the reply can talk about names without a second renderer.
 *
 * Answers 0 when the key does not belong to a container, which is the caller's signal to
 * use the whole key as it always did.
 */
unsigned encoded_container_name_len(art::value_type key) {
    if (!key.size || !art::is_container_lead(*key.bytes)) {
        return 0;
    }
    if (key.size < 3) return 0;
    const char *ptr = (const char *) &key.bytes[2];
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
        case art::tstring:
            len = encoded_str_len(ptr + 1, key.size - 2) + 2;
            break;
        default:
            return 0;
    }
    if (2u + len > key.size) return 0;
    return 2u + len;
}

/**
 * Is this key a container's own bookkeeping rather than something a caller named?
 *
 * An ordered set keeps a second range, member to score, whose keys begin with the kind's
 * lead byte and then an empty component before the name. `encoded_container_name_len`
 * cannot measure a name there because there is none in the first position, and every
 * caller that reports names took that zero to mean "an ordinary key" - so KEYS answered a
 * set twice, once as itself and once as a nonsense name made of its index bytes.
 */
/**
 * The name a container key belongs to, decoded.
 *
 * Every key of a container carries the name, but not always in the same place: an ordered
 * set's member index puts an empty component first, so the name sits second. A compiled
 * probe settled what that looks like - an empty component is a bare type byte with no
 * content and no terminator, `0a 01 03`, and the component after it follows immediately:
 *
 *     score key: 0a 01 03 7a 01 | 02 02 4079... | 03 61 00
 *     index key: 0a 01 03 | 03 7a 01 | 03 61 00
 *
 * The useful part is that the decoder steps over the empty component on its own, so both
 * of those decode to the same name. Reporting that string, rather than a slice of the
 * bytes, makes the two keys agree - which is what stops an ordered set being listed twice,
 * once as itself and once as a phantom made of its index bytes.
 */
std::string encoded_container_name(art::value_type key) {
    if (!key.size || !art::is_container_lead(*key.bytes)) {
        return {};
    }
    unsigned nl = encoded_container_name_len(key);
    if (!nl) {
        return {};
    }
    std::string framed;
    framed.push_back((char) art::tcomposite);
    framed.push_back('\x01');
    framed.append((const char *) key.bytes + 2, nl - 2);
    return encoded_key_as_string(art::value_type{framed});
}

bool is_container_internal(art::value_type key) {
    if (!key.size || !art::is_container_lead(*key.bytes)) {
        return false;
    }
    return encoded_container_name_len(key) == 0;
}

std::string encoded_key_as_string(art::value_type key, char sep) {
    Variable v = encoded_key_as_variant(key, sep);
    return v.s();
}
unsigned log_encoded_key(art::value_type key, bool start) {
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
    if (start) barch::std_start();
    if (key_len >= numeric_key_size && (*enck == art::tinteger || *enck == art::tdouble)) {
        ik = conversion::enc_bytes_to_int(enck, numeric_key_size);
        if (*enck == art::tdouble) {
            memcpy(&dk, &ik, sizeof(ik));
            barch::std_continue("{ double }[", dk, "]");
            if (start) barch::std_end();
            return numeric_key_size;
        } else {
            barch::std_continue("{ integer }[", ik, "]");
            if (start) barch::std_end();
            return numeric_key_size;
        }
    } else if (key_len >= num32_key_size && (*enck == art::tshort || *enck == art::tfloat)) {
        sk = conversion::enc_bytes_to_int32(enck, num32_key_size);
        if (*enck == art::tfloat) {
            memcpy(&fk, &sk, sizeof(sk));
            barch::std_continue("{ float }[", fk, "]");
            if (start) barch::std_end();
            return num32_key_size;
        } else {
            barch::std_continue("{ short }[", sk, "]");
            if (start) barch::std_end();
            return num32_key_size;
        }
    } else if (key_len >= 1 && *enck == art::tstring) {
        k = (const char *) &enck[1];
        kl = key_len - 2;
        std::string s;
        s.insert(s.end(), k, k + kl);
        barch::std_continue("{ string }[", s, "][", kl, "]");
        if (start) barch::std_end();
        return 2 + kl;
    } else if (key_len > 1 && art::is_composite_lead(*enck)) {
        barch::std_continue("{ composite [2] }[");
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
                    barch::std_continue("<key or data error>");
                    if (start) barch::std_end();
                    return 0;
            }
            barch::std_continue("[", len, "]");
            ptr += log_encoded_key({ptr, len}, false);
            kl += len;
        }
        barch::std_continue("] [", kl, "]");
        if (start) barch::std_end();
        return key_len;
    }
    if (start) barch::std_end();

    return 0;
}
