//
// Created by teejip on 6/9/25.
//

#ifndef KEY_OPTIONS_H
#define KEY_OPTIONS_H
#include <cstdint>
#include "ioutil.h"
#include "keyspec.h"
namespace art {
    struct key_options {
        enum {
            flag_is_keep_ttl = 1,
            flag_is_volatile = 2,
            flag_has_expiry = 4,
            flag_is_hashed = 8,
        };

        key_options() = default;
        key_options(int64_t expiry, bool keep_ttl, bool is_volatile, bool is_hashed) : expiry(expiry), flags(0) {
            if (keep_ttl) {
                flags |= flag_is_keep_ttl;
            }
            if (is_volatile) {
                flags |= flag_is_volatile;
            }
            if (expiry) {
                flags |= flag_has_expiry;
            }
            if (is_hashed) {
                flags |= flag_is_hashed;
            }
        }
        void writep(std::ostream& out) const {
            ::writep(out, htonl(expiry));
            ::writep(out, flags);
        }
        void readp(std::istream& in) {
            ::readp(in, expiry);
            ::readp(in, flags);
            expiry = ntohl(expiry);
        }
        key_options(const key_spec & s) {
            expiry = s.ttl;
            set_keep_ttl(s.keepttl);
            set_hashed(s.hash);
            //set_is_volatile(s.is_volatile);
        }
        bool is_keep_ttl() const {
            return flags & flag_is_keep_ttl;
        }
        bool is_volatile() const {
            return flags & flag_is_volatile;
        }
        bool is_hashed() const {
            return flags & flag_is_hashed;
        }
        bool has_expiry() const {
            return flags & flag_has_expiry;
        }
        void set_expiry(uint64_t ttl) {
            this->expiry = ttl;
            set_has_expiry(ttl > 0);
        }
        uint64_t get_expiry() const {
            return expiry;
        }
        void set_has_expiry(bool truth) {
            if (truth) {
                flags |= flag_has_expiry;
            } else {
                flags &= ~flag_has_expiry;
            }
        }
        void set_keep_ttl(bool truth) {
            if (truth) {
                flags |= flag_is_keep_ttl;
            } else {
                flags &= ~flag_is_keep_ttl;
            }
        }
        void set_volatile(bool truth) {
            if (truth) {
                flags |= flag_is_volatile;
            }else {
                flags &= ~flag_is_volatile;
            }
        }
        void set_hashed(bool truth) {
            if (truth) {
                flags |= flag_is_hashed;
            }else {
                flags &= ~flag_is_hashed;
            }
        }

    private:
        uint64_t expiry{};
    public:
        uint8_t flags{flag_is_keep_ttl};
    };
};
#endif //KEY_OPTIONS_H
