//
// Created by linuxlite on 3/21/25.
//

#ifndef SET_H
#define SET_H

extern "C" {
#include "valkeymodule.h"
}

#include <string>
#include <regex>
#include <chrono>
#include <initializer_list>
#include "sastam.h"
namespace art
{
    /**
         * the current time in milliseconds
         */
    static int64_t now()
    {
        using namespace std::chrono;
        return std::chrono::duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    }

    struct base_key_spec
    {
        ValkeyModuleString* const* argv{};
        static std::regex integer;
        int argc = 0;
        int r = 0;
        char empty[2] = {0x00,0x00};
        mutable std::string s{};

        const std::string& tos(int at) const
        {
            s.clear();
            if (at >= argc) return s;
            size_t vlen = 0;
            const char* val = ValkeyModule_StringPtrLen(argv[at], &vlen);
            if (val == nullptr)
            {
                return s;
            }

            s.append(val, vlen);
            return s;
        }

        const char * toc(int at) const
        {
            size_t vlen = 0;
            const char* val = ValkeyModule_StringPtrLen(argv[at], &vlen);
            if (val == nullptr)
            {
                return empty;
            }
            return val;
        }
        std::string& tos(int at)
        {
            s.clear();
            if (at >= argc) return s;

            size_t vlen = 0;
            const char* val = ValkeyModule_StringPtrLen(argv[at], &vlen);
            if (val == nullptr)
            {
                return s;
            }

            s.append(val, vlen);
            return s;
        }

        bool is_integer(int at)
        {
            if (at >= argc) return false;

            auto& scheck = tos(at);
            return  std::regex_match(scheck, integer);
        }
        // integer
        int64_t tol(int at) const
        {
            if (at >= argc) return 0;

            auto& scheck = tos(at);
            if (!std::regex_match(scheck, integer))
            {
                return 0;
            }
            auto val = std::stoll(scheck);
            return val;
        }
        static std::string& to_lower(std::string& s)
        {
            std::transform(s.begin(), s.end(), s.begin(), ::tolower);
            return s;
        }
        static std::string to_lower(const std::string& s)
        {
            std::string l = s;
            std::transform(l.begin(), l.end(), l.begin(), ::tolower);

            return l;
        }

        int has_enum(const std::initializer_list<const char*>& names,int at)
        {

            const char * token = toc(at);
            int matches = 0;
            for (const char* name : names)
            {
                if (*token == std::tolower(*name) || *token == std::toupper(*name))
                {
                    ++matches;
                }
            }
            if (!matches) return (int)names.size();

            const std::string& it = tos(at);
            if (it.empty()) return false;
            to_lower(s);
            int found = 0;
            for (auto& n : names)
            {
                if (to_lower(n) == s)
                {
                    return found;
                }
                ++found;
            }
            return found;
        }
        template<class T>
        int has_enum(const T& names,int at)
        {
            const std::string& it = tos(at);
            if (it.empty()) return false;
            to_lower(s);
            int found = 0;
            for (auto& n : names)
            {
                if (to_lower(n) == s)
                {
                    return found;
                }
                ++found;
            }
            return found;
        }

        bool has(const char* what, int at) const
        {
            if (at >= argc) return false;

            const std::string& it = tos(at);
            if (it.empty()) return false;
            std::transform(s.begin(), s.end(), s.begin(), ::tolower);
            return s == what;
        }
    };

    struct key_spec : base_key_spec
    {
        bool none = false;
        bool get = false;
        bool nx = false;
        bool xx = false;
        bool keepttl = true;
        int64_t ttl = 0;

        key_spec() = default;

        key_spec(ValkeyModuleString** argvz, int argcz)
        {
            argv = argvz;
            argc = argcz;
        }

        key_spec& operator=(ValkeyModuleString**) = delete;
        key_spec& operator=(const key_spec&) = delete;
        key_spec(const key_spec&) = delete;

        int parse_options()
        {
            if (argc < 3)
            {
                none = true;
                return VALKEYMODULE_OK;
            }
            int spos = 3; // the keys are one and two
            get = has("get", spos);
            if (get) ++spos;

            if (argc <= spos)
                return VALKEYMODULE_OK;

            nx = has("nx", spos);

            if (!nx)
            {
                xx = has("xx", spos);
                if (xx) ++spos;
            }
            else
            {
                ++spos;
            }

            if (argc <= spos)
                return VALKEYMODULE_OK;

            if (has("ex", spos) || has("px", spos))
            {
                if (argc <= spos + 1)
                    return VALKEYMODULE_ERR;

                if (has("ex", spos))
                {
                    ttl = tol(++spos) * 1000 + now();
                }
                else
                {
                    ttl = tol(++spos) + now();
                }
                ++spos;
            }
            else if (has("exat", spos) || has("pxat", spos))
            {
                if (argc <= spos + 1)
                    return VALKEYMODULE_ERR;

                if (has("exat", spos))
                {
                    ttl = tol(++spos) * 1000;
                }
                else
                {
                    ttl = tol(++spos);
                }
                ++spos;
            }
            if (has("keepttl", spos))
            {
                keepttl = true;
                ++spos;
            }
            if (argc == spos) // all known arguments should be consumed
                return VALKEYMODULE_OK;

            return VALKEYMODULE_ERR;
        }
    };

    struct keys_spec : base_key_spec
    {
        keys_spec& operator=(ValkeyModuleString**) = delete;
        keys_spec& operator=(const keys_spec&) = delete;
        keys_spec(const keys_spec&) = delete;

        keys_spec(ValkeyModuleString** argvz, int argcz)
        {
            argv = argvz;
            argc = argcz;
        }

        bool count = false;
        int64_t max_count{std::numeric_limits<int64_t>::max()};

        int parse_keys_options()
        {
            int spos = 2; // the pattern is the first one
            if (argc < 3)
            {
                return VALKEYMODULE_OK;
            }
            if (has("count", spos))
            {
                count = true;
                ++spos;
            }
            if (has("max", spos))
            {
                max_count = tol(++spos);
                ++spos;
            }

            if (argc == spos) // all known arguments should be consumed
                return VALKEYMODULE_OK;

            return VALKEYMODULE_ERR;
        }
    };
    struct hexpire_spec : base_key_spec
    {
        hexpire_spec& operator=(ValkeyModuleString**) = delete;
        hexpire_spec& operator=(const keys_spec&) = delete;
        hexpire_spec(const hexpire_spec&) = delete;

        hexpire_spec(ValkeyModuleString** argvz, int argcz)
        {
            argv = argvz;
            argc = argcz;
        }
        bool NX{false};
        bool XX{false};
        bool GT{false};
        bool LT{false};
        int64_t which_flag{4};
        int64_t fields{0};
        int64_t seconds{0};
        int fields_start{0};
        int parse_options()
        {
            int spos = 2; // the pattern is the first one
            if (argc < 3)
            {
                return VALKEYMODULE_OK;
            }
            if (is_integer(spos))
            {
                seconds = tol(spos++);
            }

            which_flag = has_enum({"nx","xx","gt","lt"}, spos);
            if (which_flag < 4)
            {
                switch (which_flag)
                {
                    case 0:
                        NX = true;
                        break;
                    case 1:
                        XX = true;
                        break;
                    case 2:
                        GT = true;
                        break;
                    case 3:
                        LT = true;
                        break;
                    default:
                        break;
                }
                ++spos;
            }
            if (has("fields",spos))
            {
                if (!is_integer(++spos))
                {
                    return VALKEYMODULE_ERR;
                }
                fields = tol(spos++);
                if (fields != argc - spos)
                {
                    return VALKEYMODULE_ERR;
                }
                fields_start = spos;
            }else
            {
                return VALKEYMODULE_ERR;
            }
            return VALKEYMODULE_OK;
        }
    };
    struct hgetex_spec : base_key_spec
    {
        hgetex_spec& operator=(ValkeyModuleString**) = delete;
        hgetex_spec& operator=(const hgetex_spec&) = delete;
        hgetex_spec(const hgetex_spec&) = delete;

        hgetex_spec(ValkeyModuleString** argvz, int argcz)
        {
            argv = argvz;
            argc = argcz;
        }
        bool EX{false};
        bool PX{false};
        bool EXAT{false};
        bool PXAT{false};
        bool PERSIST{false};
        int64_t which_flag{5};
        int64_t fields{0};
        int64_t time_val{0};
        int fields_start{0};
        int parse_options()
        {
            int spos = 2; // the hash name is the first one
            if (argc < 3)
            {
                return VALKEYMODULE_OK;
            }

            which_flag = has_enum({"ex","px","exat","pxat", "persist"}, spos);
            if (which_flag < 5)
            {
                switch (which_flag)
                {
                    case 0:
                        EX = true;
                        break;
                    case 1:
                        PX = true;
                        break;
                    case 2:
                        EX = true;
                        break;
                    case 3:
                        EXAT = true;
                        break;
                    case 4:
                        PERSIST = true;
                    default:
                        break;
                }
                if (is_integer(++spos))
                {
                    time_val = tol(spos);
                    ++spos;
                }

            }
            if (has("fields",spos))
            {
                if (!is_integer(++spos))
                {
                    return VALKEYMODULE_ERR;
                }
                fields = tol(spos++);
                if (fields != argc - spos)
                {
                    return VALKEYMODULE_ERR;
                }
                fields_start = spos;
            }else
            {
                return VALKEYMODULE_ERR;
            }
            return VALKEYMODULE_OK;
        }
    };
    struct httl_spec : base_key_spec
    {
        httl_spec& operator=(ValkeyModuleString**) = delete;
        httl_spec& operator=(const httl_spec&) = delete;
        httl_spec(const hgetex_spec&) = delete;

        httl_spec(ValkeyModuleString** argvz, int argcz)
        {
            argv = argvz;
            argc = argcz;
        }
        int64_t fields{0};
        int64_t time_val{0};
        int fields_start{0};
        int parse_options()
        {
            int spos = 2; // the hash name is the first one
            if (argc < 3)
            {
                return VALKEYMODULE_OK;
            }

            if (has("fields",spos))
            {
                if (!is_integer(++spos))
                {
                    return VALKEYMODULE_ERR;
                }
                fields = tol(spos++);
                if (fields != argc - spos)
                {
                    return VALKEYMODULE_ERR;
                }
                fields_start = spos;
            }else
            {
                return VALKEYMODULE_ERR;
            }
            return VALKEYMODULE_OK;
        }
    };
    struct zadd_spec : base_key_spec
    {
        zadd_spec& operator=(ValkeyModuleString**) = delete;
        zadd_spec& operator=(const zadd_spec&) = delete;
        zadd_spec(const zadd_spec&) = delete;

        zadd_spec(ValkeyModuleString** argvz, int argcz)
        {
            argv = argvz;
            argc = argcz;
        }
        bool NX{false};
        bool XX{false};
        bool GT{false};
        bool LT{false};
        bool CH{false};
        bool LFI{false};

        int64_t which_flag_n{3};
        int64_t which_flag_g{3};

        int fields_start{0};
        int parse_options()
        {
            int spos = 2; // the key is the first one
            if (argc < 3)
            {
                return VALKEYMODULE_ERR;
            }
            which_flag_n = has_enum({"nx","xx"}, spos);
            if (which_flag_n < 2)
            {
                switch (which_flag_n)
                {
                case 0:
                    NX = true;
                    break;
                case 1:
                    XX = true;
                    break;
                default:
                    break;
                }
                ++spos;
            }

            which_flag_g = has_enum({"gt","lt"}, spos);
            if (which_flag_g < 2)
            {
                switch (which_flag_g)
                {
                case 0:
                    GT = true;
                    break;
                case 1:
                    LT = true;
                    break;
                default:
                    break;
                }
                ++spos;

            }
            while (true)
            {

                int lfi_ch = has_enum({"ch","lfi"}, spos);
                if ( lfi_ch == 0)
                {
                    CH = true;
                    spos++;
                }
                if (lfi_ch == 1){
                    LFI = true;
                    spos++;
                }
                if (lfi_ch == 2) break;

            }
            fields_start = spos;
            return VALKEYMODULE_OK;
        }
    };
    struct zops_spec : base_key_spec
    {
        zops_spec& operator=(ValkeyModuleString**) = delete;
        zops_spec& operator=(const zops_spec&) = delete;
        zops_spec(const zops_spec&) = delete;

        zops_spec(ValkeyModuleString** argvz, int argcz)
        {
            argv = argvz;
            argc = argcz;
        }
        int64_t fields_start{0};
        size_t numkeys{0};
        heap::std_vector<std::string> keys{};
        heap::std_vector<double> weight_values{};
        heap::std_vector<std::string> keywords = {"weights","aggregate","withscores"};
        heap::std_vector<std::string> aggregate_types = {"sum","min","max", "avg"};

        enum keyword_index
        {
            weights = 0,
            aggregate = 1,
            withscores = 2
        };
        enum aggregate_index
        {
            sum = 0,
            min = 1,
            max = 2,
            avg = 3,
            agg_none = 4
        };
        bool has_withscores{false};
        aggregate_index aggr{agg_none};
        aggregate_index map_aggr(int ix)
        {
            switch (ix)
            {
                case 0:
                    return sum;
                case 1:
                    return min;
                case 2:
                    return max;
            }
            return agg_none;
        }
        int parse_options()
        {
            int spos = 1; // the key is the first one
            if (argc < 4)
            {
                return VALKEYMODULE_ERR;
            }
            if (is_integer(spos))
            {
                numkeys = tol(spos++);
            }else
            {
                return VALKEYMODULE_ERR;
            }

            while (keys.size() < numkeys)
            {
                if (spos >= argc)
                {
                    return VALKEYMODULE_ERR;
                }
                keys.push_back(tos(spos++));
            }
            if (keys.size() != numkeys)
            {
                return VALKEYMODULE_ERR;
            }
            if (spos == argc)
            {
                return VALKEYMODULE_OK;
            }
            do
            {
                int which = has_enum(keywords,spos);
                switch (which)
                {
                case weights:
                    ++spos;
                    if (!weight_values.empty())
                    {
                        return VALKEYMODULE_ERR;
                    }
                    while (is_integer(spos))
                    {
                        weight_values.push_back(tol(spos++));
                    }

                    break;
                case aggregate:
                    if (aggr != agg_none)
                    {
                        return VALKEYMODULE_ERR;
                    }
                    aggr = map_aggr(has_enum(aggregate_types,++spos));
                    ++spos;
                    break;
                case withscores:
                    if (has_withscores)
                    {
                        return VALKEYMODULE_ERR;
                    }
                    has_withscores = true;
                    ++spos;
                    break;
                default:
                    return VALKEYMODULE_ERR;
                }
            } while (spos < argc);
            if (has_withscores && aggr != agg_none)
            {
                return VALKEYMODULE_ERR;
            }
            return VALKEYMODULE_OK;
        }
    };
    struct zrange_spec : base_key_spec
    {
        zrange_spec& operator=(ValkeyModuleString**) = delete;
        zrange_spec& operator=(const zrange_spec&) = delete;
        zrange_spec(const zops_spec&) = delete;

        zrange_spec(ValkeyModuleString** argvz, int argcz)
        {
            argv = argvz;
            argc = argcz;
        }
        int64_t fields_start{0};
        size_t numkeys{0};
        std::string key{};
        std::string start{};
        std::string stop{};

        heap::std_vector<std::string>& get_kewords()
        {
            static heap::std_vector<std::string> by = {"byscore","bylex","rev","limit", "withscores"};
            return by;
        }

        enum keyword_index
        {
            byscore = 0,
            bylex = 1,
            rev = 2,
            limit = 3,
            withscores = 4
        };
        bool BYLEX{false};
        bool REMOVE{false};
        bool BYSCORE{false};
        bool REV{false};
        bool has_withscores{false};
        int64_t offset{0};
        int64_t count{0};
        int parse_options()
        {
            int spos = 1; // the key is the first one
            if (argc < 4)
            {
                return VALKEYMODULE_ERR;
            }
            key = tos(spos++);
            if (spos == argc)
            {
                return VALKEYMODULE_ERR;
            }
            start = tos(spos++);
            if (spos == argc)
            {
                return VALKEYMODULE_ERR;
            }
            stop = tos(spos++);
            if (spos == argc)
            {
                return VALKEYMODULE_OK;
            }

            do
            {
                int which = has_enum(get_kewords(),spos);
                switch (which)
                {
                case byscore:
                    ++spos;
                    BYSCORE = true;
                    break;
                case bylex:
                    ++spos;
                    BYLEX = true;
                    break;
                case limit:
                    ++spos;
                    if (!is_integer(spos))
                    {
                        return VALKEYMODULE_ERR;
                    }
                    offset = tol(spos++);
                    if (!is_integer(spos))
                    {
                        return VALKEYMODULE_ERR;
                    }
                    count = tol(spos++);
                    break;
                case rev:
                    ++spos;
                    REV = true;
                    break;
                case withscores:
                    if (has_withscores)
                    {
                        return VALKEYMODULE_ERR;
                    }
                    has_withscores = true;
                    ++spos;
                    break;
                default:
                    return VALKEYMODULE_ERR;
                }
            } while (spos < argc);
            return VALKEYMODULE_OK;
        }
    };
};
#endif //SET_H
