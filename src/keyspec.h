//
// Created by linuxlite on 3/21/25.
//

#ifndef SET_H
#define SET_H
extern "C"
{
#include "valkeymodule.h"
}

#include "art.h"
#include <string>
#include <vector>
namespace art{
    struct key_spec {
        ValkeyModuleString **argv;
        int argc;

        bool none = false;
        bool get = false;
        bool nx = false;
        bool xx = false;
        int64_t ex = 0;

        mutable std::string s;
        int64_t now() const
        {
            return 0;
        }
        const std::string& tos(int at) const
        {
            s.clear();
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
            size_t vlen = 0;
            const char * val = ValkeyModule_StringPtrLen(argv[at], &vlen);
            if(val == nullptr)
            {
                return s;
            }

            s.append(val, vlen);
            return s;
        }
        int64_t tol(int at) const
        {
            return std::stoll(tos(at));
        }
        bool has(const char * what, int at) const {
            const std::string &it = tos(at);
            if (it.empty()) return false;
            std::transform(s.begin(), s.end(), s.begin(), ::tolower);
            return s == what;
        }

        int parse_set_options(ValkeyModuleString **argv, int argc){
            if (argc < 4)
            {   none = true;
                return VALKEYMODULE_OK;
            }
            this->argv = argv;
            this->argc = argc;
            int s = 4;
            get = has("get",s);
            if (get) ++s;
            if (argc <= s)
                return VALKEYMODULE_OK;
            nx = has("nx",s);

            if (!nx)
            {
                xx = has("xx",s);
                if (xx) ++s;

            } else
            {
                ++s;
            }

            if (argc <= s)
                return VALKEYMODULE_OK;

            if (has("ex",s) || has("px",s))
            {
                if (argc <= s+1)
                    return VALKEYMODULE_ERR;

                if (has("ex",s))
                {
                    ex = tol(++s)*1000 + now();
                } else
                {
                    ex = tol(++s) + now();
                }
            }

            if (argc <= s)
                return VALKEYMODULE_OK;

            if (has("exat",s) || has("pxat",s))
            {
                if (argc <= s + 1)
                    return VALKEYMODULE_ERR;

                if (has("ex",s))
                {
                    ex = tol(++s)*1000;
                } else
                {
                    ex = tol(++s);
                }
            }
            return VALKEYMODULE_OK;
        }
    };
};
#endif //SET_H
