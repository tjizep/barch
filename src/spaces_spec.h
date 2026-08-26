//
// Created by teejip on 10/28/25.
//

#ifndef BARCH_SPACES_SPEC_H
#define BARCH_SPACES_SPEC_H
#include "keyspec.h"
namespace art {
        struct kspace_spec : base_key_spec {
        kspace_spec &operator=(ValkeyModuleString **) = delete;

        kspace_spec &operator=(const kspace_spec &) = delete;

        kspace_spec(const kspace_spec &) = delete;

        kspace_spec(const arg_t& argv) :base_key_spec(argv) {

        }
        kspace_spec() :base_key_spec(arg_t{}) {

        }
        bool is_depends = false;
        bool is_dependants = false;
        bool is_release = false;
        bool is_merge = false;
        bool is_merge_default = false;
        bool is_merge_compress = false;
        bool is_option = false;
        bool is_get = false;
        bool is_set = false;
        bool is_static = false;
        bool is_drop = false;
        bool is_exist = false;
        /** KSPACE ACL [KSNAME] SETUSER|GETUSER|DEL ... - see TODO 135 */
        bool is_acl = false;
        unsigned acl_at = 0;      // where the acl_spec tail begins

        std::string dependant;
        std::string source;
        std::string name;
        std::string value;
        int parse_options() {
            clear_error();
            unsigned spos = 1; // the pattern is the first one
            if (has("ACL", spos)) {
                is_acl = true;
                ++spos;
                /*
                 * Verb first, name after: every other KSPACE subcommand puts its verb
                 * at spos 1 and this parser reads it there. Name first would need a
                 * look ahead and would be ambiguous against a space actually called
                 * `acl`. The name is optional and means the selected space, which is
                 * unambiguous because what follows it is always SETUSER, GETUSER or
                 * DEL.
                 */
                if (!has("SETUSER", spos) && !has("GETUSER", spos) && !has("DEL", spos)) {
                    name = tos(spos);
                    if (!barch::check_ks_name(name)) {
                        return -1;
                    }
                    ++spos;
                }
                acl_at = spos;
                return 0;
            }
            if (has("EXIST", spos)) {
                is_exist = true;
                ++spos;
                name = tos(spos);
                if (!barch::check_ks_name(name)) {
                    return -1;
                }
                return is_parse_error(spos);
            }
            if (has("DEPENDS", spos)) {
                is_depends = true;
                ++spos;
                dependant = tos(spos);
                if (!barch::check_ks_name(dependant)) {
                    return -1;
                }
                ++spos;
                if (has("ON", spos)) {
                    source = tos(++spos);
                    if (!barch::check_ks_name(source)) {
                        return -1;
                    }
                    if (has("STATIC", spos)) {
                        is_static = true;
                        ++spos;
                    }
                    return is_parse_error(spos);
                }
                return -1; // it's an error
            }

            if (has("DEPENDANTS", spos)) {
                is_dependants = true;
                source = tos(++spos);
                return is_parse_error(spos);
            }

            if (has("RELEASE",spos)) {
                is_release = true;
                source = tos(++spos);
                if (!barch::check_ks_name(dependant)) {
                    return -1;
                }
                ++spos;
                if (has("FROM", spos)) {
                    dependant = tos(++spos);
                    if (!barch::check_ks_name(source)) {
                        return -1;
                    }
                    return is_parse_error(spos);
                }
                return -1;
            }
            if (has("DROP",spos)) {
                is_drop = true;
                source = tos(++spos);

                return is_parse_error(spos);
            }

            if (has("MERGE",spos)) {
                is_merge = true;
                if (spos + 1 == argc) {
                    is_merge_default = true;
                    return 0;
                }
                dependant = tos(++spos);
                if (!barch::check_ks_name(dependant)) {
                    return -1;
                }
                ++spos;
                if (has("INTO", spos)) {
                    source = tos(++spos);
                    if (!barch::check_ks_name(source)) {
                        return -1;
                    }
                    if (has("COMPRESS", spos+1)) {
                        ++spos;
                        is_merge_compress = true;
                    }
                    return is_parse_error(spos);
                }
                return -1;
            }

            if (has("OPTION", spos)) {
                is_option = true;
                ++spos;
                if (has("SET", spos)) {
                    is_set = true;
                    if (has_enum({"ORDERED","LRU","RANDOM"},++spos) < 3) {
                        name = tos(spos);
                    }else {
                        return -1;
                    }
                    if (has_enum({"ON","OFF","VOLATILE"},++spos) < 3) {
                        value = tos(spos);
                    } else {
                        return -1;
                    }

                    return is_parse_error(spos);
                }
                if (has("GET",spos)) {
                    is_get = true;
                    // the count is the length of the list above it - a name added
                    // without moving it parses as a syntax error
                    if (has_enum({"ORDERED","LRU","RANDOM",
                                  "FOREIGN","MISSING_TTL","FOREIGN_TIMEOUT",
                                  "FOREIGN_QUERY_TIMEOUT","FOREIGN_INFLIGHT",
                                  "KEY_SPLIT","FOREIGN_POOL_MAX_AGE",
                                  "FUNCTION_SLICE","FUNCTION_DEADLINE"},++spos) < 12) {
                        name = tos(spos);
                    }else {
                        return -1;
                    }

                    return is_parse_error(spos);
                }
                return -1;
            }
            return -1;
        }
    };

}
#endif //BARCH_SPACES_SPEC_H