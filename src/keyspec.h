//
// Created by linuxlite on 3/21/25.
//

#ifndef SET_H
#define SET_H

extern "C"
{
#include "valkeymodule.h"
}

#include <string>
#include <vector>
#include <regex>
#include <chrono>
namespace art {
    struct key_spec {
        ValkeyModuleString *const*argv{};
        static std::regex integer;
        int argc = 0;
        int r = 0;

        bool none = false;
        bool get = false;
        bool nx = false;
        bool xx = false;
        bool keepttl = false;
        int64_t ttl = 0;

        mutable std::string s{};
        int64_t now() const
        {
            return std::chrono::steady_clock::now().time_since_epoch().count();
        }
        const std::string& tos(int at) const
        {
            s.clear();
            if (at >= argc) return s;
            size_t vlen = 0;
            const char * val = ValkeyModule_StringPtrLen(argv[at], &vlen);
            if(val == nullptr)
            {
                return s;
            }

            s.append(val, vlen);
            return s;
        }
        std::string& tos(int at)
        {
            s.clear();
            if (at >= argc) return s;

            size_t vlen = 0;
            const char * val = ValkeyModule_StringPtrLen(argv[at], &vlen);
            if(val == nullptr)
            {
                return s;
            }

            s.append(val, vlen);
            return s;
        }
        // integer
        int64_t tol(int at) const
        {
            if (at >= argc) return 0;

            auto &scheck = tos(at);
            if (!std::regex_match(scheck, integer))
            {
                return 0;
            }
            auto val = std::stoll(scheck);
            return val;
        }
        bool has(const char * what, int at) const {
            if (at >= argc) return false;

            const std::string &it = tos(at);
            if (it.empty()) return false;
            std::transform(s.begin(), s.end(), s.begin(), ::tolower);
            return s == what;
        }
        key_spec() = default;
        key_spec(ValkeyModuleString **argvz, int argcz) {
          argv = argvz;
          argc = argcz;
        }
        key_spec& operator=(ValkeyModuleString **) = delete;
        key_spec& operator=(const key_spec&) = delete;
        key_spec(const key_spec&) = delete;
        int parse_options(){
            if (argc < 3)
            {   none = true;
                return VALKEYMODULE_OK;
            }
            int spos = 3; // the keys are one and two
            get = has("get",spos);
            if (get) ++spos;

            if (argc <= spos)
                return VALKEYMODULE_OK;

            nx = has("nx",spos);

            if (!nx)
            {
                xx = has("xx",spos);
                if (xx) ++spos;

            } else
            {
                ++spos;
            }

            if (argc <= spos)
                return VALKEYMODULE_OK;

            if (has("ex",spos) || has("px",spos))
            {
                if (argc <= spos+1)
                    return VALKEYMODULE_ERR;

                if (has("ex",spos))
                {
                    ttl = tol(++spos)*1000 + now();
                } else
                {
                    ttl = tol(++spos) + now();
                }
                ++spos;

            } else if (has("exat",spos) || has("pxat",spos))
            {
                if (argc <= spos + 1)
                    return VALKEYMODULE_ERR;

                if (has("exat",spos))
                {
                    ttl = tol(++spos)*1000;
                } else
                {
                    ttl = tol(++spos);
                }
                ++spos;
            }
            if (has("keepttl",spos))
            {
                keepttl = true;
                ++spos;
            }
            if (argc == spos) // all known arguments should be consumed
                return VALKEYMODULE_OK;

            return VALKEYMODULE_ERR;
        }
    };
};
#endif //SET_H
