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
        bool is_option = false;
        bool is_get = false;
        bool is_set = false;
        bool is_static = false;
        bool is_drop = false;

        std::string dependant;
        std::string source;
        std::string name;
        std::string value;
        int parse_options() {
            clear_error();
            int spos = 1; // the pattern is the first one

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
                    if (has_enum({"ORDERED","LRU","RANDOM"},++spos) < 3) {
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