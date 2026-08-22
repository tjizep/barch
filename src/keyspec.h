//
// Created by linuxlite on 3/21/25.
//

#ifndef SET_H
#define SET_H
//#include "conversion.h"
#include "value_type.h"
extern "C" {
    #include "../external/include/valkeymodule.h"
}

#include <string>
#include <regex>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <initializer_list>
#include "sastam.h"
#include "glob.h"

namespace barch {
    extern bool check_ks_name(const std::string& name_);
}
namespace art {



    /**
     * The current time in milliseconds since the unix epoch.
     *
     * This was `steady_clock` - milliseconds since the machine started - and every expiry
     * in the store is measured against it. Within a single run that is coherent, which is
     * why it went unnoticed: EXPIRE stored now+10000 and the key went when the clock
     * passed it. Two things were wrong with it anyway. A caller handing over an absolute
     * time, through EXAT or EXPIREAT, meant a unix time and was compared against a clock
     * counting from boot, so the deadline landed decades out; and a deadline written to a
     * shard file meant nothing after a restart, because the clock it was measured against
     * restarted too.
     *
     * The cost of a wall clock is that it can step - an NTP correction moves every
     * deadline relative to it. That is the behaviour redis has, and a stepped clock
     * expiring keys early or late is a smaller surprise than a saved deadline that means
     * something different tomorrow. See DONE 55.
     */
    static int64_t now() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

/**
 * A caller's expiry, in milliseconds since the epoch, or a refusal.
 *
 * `now() + given * 1000` overflows for a large enough EX, and a negative result is worse
 * than a wrong one here: `leaf::make_size` treats any non-zero expiry as one that needs
 * eight bytes reserved, while the leaf constructor only sets the expiry flag when it is
 * positive. The two then disagree about how big the leaf is and `make_leaf` aborts - which
 * inside valkey is a process that hangs rather than one that dies. `SET k v EX
 * 10000000000000000` did exactly that.
 *
 * redis refuses these instead, with "invalid expire time in '<command>' command", and its
 * own tests assert as much. So does this now.
 *
 * @param given    what the caller wrote
 * @param seconds  true when the units are seconds and have to be multiplied up
 * @param relative true when it is a duration from now rather than an absolute time
 */
inline bool expiry_ms(int64_t given, bool seconds, bool relative, int64_t& out) {
    constexpr int64_t limit = std::numeric_limits<int64_t>::max();
    int64_t ms = given;
    if (seconds) {
        if (given > limit / 1000 || given < -limit / 1000) return false;
        ms = given * 1000;
    }
    if (relative) {
        int64_t base = now();
        if (ms > limit - base) return false;
        ms += base;
    } else if (ms <= 0) {
        // an absolute form - EXAT, PXAT, EXPIREAT - is a unix time, and now that deadlines
        // are unix times too there is nothing to convert. A moment already past is not an
        // error: the caller is asking for the key to go, and 1 rather than 0 says so,
        // since a zero expiry means "no expiry" rather than "expired"
        out = 1;
        return true;
    }
    // A deadline already past is not an error. redis takes it as an instruction to remove
    // the key, and the callers here do that rather than refusing - see expire_command
    out = ms;
    return true;
}


    struct base_key_spec {
        arg_t argv{};
        static std::regex integer;
        unsigned argc = 0;
        unsigned r = 0;
        char empty[2] = {0x00, 0x00};
        mutable std::string s{};
        mutable int syntax_error = 0;
        unsigned is_parse_error(unsigned spos) const {
            return (syntax_error == 0 && spos + 1 == argc) ? 0 : -1;
        }
        void clear_error() {
            syntax_error = 0;
        }
        base_key_spec() = default;
        base_key_spec(const arg_t& argv):argv(argv),argc(argv.size()){};

        const std::string &tos(unsigned at) const {
            s.clear();
            if (at >= argc) {
                ++syntax_error;
                return s;
            }
            auto vt = argv[at];
            s.append(vt.chars(), vt.size);
            return s;
        }

        const char *toc(unsigned at) const {
            if (at >= argc) {
                ++syntax_error;
                return empty;
            }
           auto val = argv[at];
            if (val.empty()) {
                return empty;
            }
            return val.chars();
        }

        std::string &tos(unsigned at) {
            s.clear();
            if (at >= argc) {
                ++syntax_error;
                return s;
            }
            auto val = argv[at];
            if (val.empty()) {
                return s;
            }

            s.append(val.chars(), val.size);
            return s;
        }

        bool is_integer(unsigned at) {
            if (at >= argc) return false;

            auto &scheck = tos(at);
            return std::regex_match(scheck, integer);
        }

        // any float redis would take: strtod's parse, so inf and the exponent forms work.
        // The whole token has to be consumed - `1.5x` is not 1.5 - and NaN is refused,
        // which is what getDoubleFromObject does
        bool to_double(unsigned at, double& out) const {
            if (at >= argc) return false;
            auto &scheck = tos(at);
            if (scheck.empty()) return false;
            const char *b = scheck.c_str();
            char *end = nullptr;
            double v = std::strtod(b, &end);
            if (end == b || *end != '\0') return false;
            if (std::isnan(v)) return false;
            out = v;
            return true;
        }

        // integer
        int64_t tol(unsigned at) const {
            if (at >= argc) {
                ++syntax_error;
                return 0;
            }
            auto &scheck = tos(at);
            if (!std::regex_match(scheck, integer)) {
                ++syntax_error;
                return 0;
            }
            return std::stoll(scheck);
        }
        uint64_t toul(unsigned at) const {
            if (at >= argc) {
                ++syntax_error;
                return 0;
            }
            auto &scheck = tos(at);
            if (!std::regex_match(scheck, integer)) {
                ++syntax_error;
                return 0;
            }
            return std::stoull(scheck);
        }

        size_t has_enum(const std::initializer_list<const char *> &names, unsigned at) const {
            const char *token = toc(at);
            size_t ctr = 0;
            auto l1 = std::tolower(*token);
            for (const char *name: names) {
                if (l1 == std::tolower(*name)) {
                    if (strcasecmp(token + 1, name + 1) == 0) {
                        return ctr;
                    }
                }
                ++ctr;
            }

            return ctr;
        }
        size_t has_one(const std::initializer_list<const char *> &names, unsigned at) const {
            return has_enum(names, at) < names.size();
        }
        bool match(const char* pat, unsigned at) const {
            if (at >= argc) return false;
            const char *it = toc(at);
            return  (1 == glob::stringmatchlen({pat},{it}, true));
        }
        unsigned match(const std::initializer_list<const char *> &names, unsigned at) const {
            const char *token = toc(at);
            unsigned ctr = 0;
            for (const char *name: names) {
                if (1 == glob::stringmatchlen({name},{token}, true)){
                    return ctr;
                }
                ++ctr;
            }
            return ctr;
        }
        bool has(const char *what, unsigned at) const {
            if (at >= argc) return false;

            const char *it = toc(at);
            return strcasecmp(it, what) == 0;
        }
    };

    struct key_spec : base_key_spec {
        /** the expiry was a number, but not one that can be a deadline - see expiry_ms */
        bool bad_expire = false;
        bool none = false;
        bool get = false;
        bool nx = false;
        bool xx = false;
        bool keepttl = false;
        bool hash = false;
        int64_t ttl = 0;

        key_spec() = default;

        key_spec(const arg_t& vt) : base_key_spec(vt) {
            argc = vt.size();
        }

        key_spec &operator=(ValkeyModuleString **) = delete;

        key_spec &operator=(const key_spec &) = delete;

        key_spec(const key_spec &) = delete;

        /**
         * SET's options, in any order.
         *
         * This used to walk a cursor through fixed positions, so the options had to
         * arrive in one particular sequence: `SET k v GET NX` was accepted and
         * `SET k v NX GET` was a syntax error, which no redis client would predict.
         * It now loops over whatever is left after the key and the value and takes each
         * word as it comes, refusing a word it does not know and a duplicate or
         * contradictory pair. See TODO 38.
         *
         * The `H` flag that used to be parsed here is gone. It set `hash`, which SET then
         * overwrote from the key space's own setting on the next line, so it never did
         * anything; redis has no such option and it is now refused like any other unknown
         * word.
         */
        int parse_options() {
            if (argc < 3) {
                none = true;
                return VALKEYMODULE_OK;
            }
            bool seen_ttl = false;
            unsigned spos = 3; // the key is one and the value is two
            while (spos < argc) {
                if (has("get", spos)) {
                    if (get) return VALKEYMODULE_ERR;
                    get = true;
                    ++spos;
                    continue;
                }
                if (has("nx", spos)) {
                    if (nx || xx) return VALKEYMODULE_ERR;
                    nx = true;
                    ++spos;
                    continue;
                }
                if (has("xx", spos)) {
                    if (nx || xx) return VALKEYMODULE_ERR;
                    xx = true;
                    ++spos;
                    continue;
                }
                if (has("keepttl", spos)) {
                    if (keepttl || seen_ttl) return VALKEYMODULE_ERR;
                    keepttl = true;
                    ++spos;
                    continue;
                }
                const bool ex   = has("ex", spos);
                const bool px   = has("px", spos);
                const bool exat = has("exat", spos);
                const bool pxat = has("pxat", spos);
                if (ex || px || exat || pxat) {
                    if (seen_ttl || keepttl) return VALKEYMODULE_ERR;
                    if (argc <= spos + 1) return VALKEYMODULE_ERR;
                    if (!is_integer(spos + 1)) return VALKEYMODULE_ERR;
                    int64_t given = tol(spos + 1);
                    if (!expiry_ms(given, ex || exat, ex || px, ttl)) {
                        bad_expire = true;
                        return VALKEYMODULE_ERR;
                    }
                    seen_ttl = true;
                    spos += 2;
                    continue;
                }
                return VALKEYMODULE_ERR; // a word we do not know
            }
            return VALKEYMODULE_OK;
        }
    };

    struct key_expire_spec : base_key_spec {
        /** the expiry was a number, but not one that can be a deadline - see expiry_ms */
        bool bad_expire = false;
        /** what was wrong, when the parse can say something better than "syntax error" */
        std::string reason;
        /** EXPIRE reads seconds from now; PEXPIRE, EXPIREAT and PEXPIREAT vary both axes */
        bool millis = false;
        bool absolute = false;
        bool nx = false;
        bool xx = false;
        bool gt = false;
        bool lt = false;
        int64_t ttl = 0;

        key_expire_spec() = default;

        key_expire_spec(const arg_t& vt) : base_key_spec(vt) {
            argc = vt.size();
        }

        key_spec &operator=(ValkeyModuleString **) = delete;

        key_spec &operator=(const key_spec &) = delete;

        key_expire_spec(const key_spec &) = delete;

        int parse_options() {

            unsigned spos = 2; // the keys at one
            if (!is_integer(spos)) {
                // an empty string, or a word - redis calls that a bad number rather than
                // a bad command, and says which
                reason = "value is not an integer or out of range";
                return VALKEYMODULE_ERR;
            }

            if (!expiry_ms(tol(spos++), !millis, !absolute, ttl)) {
                bad_expire = true;
                return VALKEYMODULE_ERR;
            }

            if (argc <= spos)
                return VALKEYMODULE_OK;

            // every remaining word is a condition. Only one used to be read, and anything
            // after it was refused, so `EXPIRE k 10 NX GT` could not say what was wrong
            // with it - and what is wrong with it is that redis names the combination
            while (spos < argc) {
                if (has("nx", spos)) nx = true;
                else if (has("xx", spos)) xx = true;
                else if (has("gt", spos)) gt = true;
                else if (has("lt", spos)) lt = true;
                else {
                    // the word in this position used to be consumed whatever it said, so
                    // `EXPIRE k 10 NONSENSE` was quietly taken as no condition at all
                    reason = "Unsupported option " + tos(spos);
                    return VALKEYMODULE_ERR;
                }
                ++spos;
            }
            if (gt && lt) {
                reason = "GT and LT options at the same time are not compatible";
                return VALKEYMODULE_ERR;
            }
            if (nx && (xx || gt || lt)) {
                reason = "NX and XX, GT or LT options at the same time are not compatible";
                return VALKEYMODULE_ERR;
            }
            return VALKEYMODULE_OK;
        }
    };

    struct keys_spec : base_key_spec {
        keys_spec &operator=(ValkeyModuleString **) = delete;

        keys_spec &operator=(const keys_spec &) = delete;

        keys_spec(const keys_spec &) = delete;

        keys_spec(const arg_t& argv) :base_key_spec(argv) {

        }
        keys_spec() :base_key_spec(arg_t{}) {

        }

        bool count = false;
        int64_t max_count{std::numeric_limits<int64_t>::max()};

        int parse_keys_options() {
            unsigned spos = 2; // the pattern is the first one
            if (argc < 3) {
                return VALKEYMODULE_OK;
            }
            if (has("count", spos)) {
                count = true;
                ++spos;
            }
            if (has("max", spos)) {
                max_count = tol(++spos);
                ++spos;
            }

            if (argc == spos) // all known arguments should be consumed
                return VALKEYMODULE_OK;

            return VALKEYMODULE_ERR;
        }
    };

    struct hexpire_spec : base_key_spec {
        hexpire_spec &operator=(ValkeyModuleString **) = delete;

        hexpire_spec &operator=(const keys_spec &) = delete;

        hexpire_spec(const hexpire_spec &) = delete;

        hexpire_spec(const arg_t& argv) : base_key_spec(argv) {
        }

        bool NX{false};
        bool XX{false};
        bool GT{false};
        bool LT{false};
        int64_t which_flag{4};
        int64_t fields{0};
        int64_t seconds{0};
        unsigned fields_start{0};

        int parse_options() {
            unsigned spos = 2; // the pattern is the first one
            if (argc < 3) {
                return VALKEYMODULE_OK;
            }
            if (is_integer(spos)) {
                seconds = tol(spos++);
            }

            which_flag = has_enum({"nx", "xx", "gt", "lt"}, spos);
            if (which_flag < 4) {
                switch (which_flag) {
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
            if (has("fields", spos)) {
                if (!is_integer(++spos)) {
                    return VALKEYMODULE_ERR;
                }
                fields = tol(spos++);
                if (fields != argc - spos) {
                    return VALKEYMODULE_ERR;
                }
                fields_start = spos;
            } else {
                return VALKEYMODULE_ERR;
            }
            return VALKEYMODULE_OK;
        }
    };

    struct hgetex_spec : base_key_spec {
        hgetex_spec &operator=(ValkeyModuleString **) = delete;

        hgetex_spec &operator=(const hgetex_spec &) = delete;

        hgetex_spec(const hgetex_spec &) = delete;

        hgetex_spec(const arg_t& argv) : base_key_spec(argv) {

        }

        bool EX{false};
        bool PX{false};
        bool EXAT{false};
        bool PXAT{false};
        bool PERSIST{false};
        int64_t which_flag{5};
        int64_t fields{0};
        int64_t time_val{0};
        unsigned fields_start{0};

        int parse_options() {
            unsigned spos = 2; // the hash name is the first one
            if (argc < 3) {
                return VALKEYMODULE_OK;
            }

            which_flag = has_enum({"ex", "px", "exat", "pxat", "persist"}, spos);
            if (which_flag < 5) {
                switch (which_flag) {
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
                if (is_integer(++spos)) {
                    time_val = tol(spos);
                    ++spos;
                }
            }
            if (has("fields", spos)) {
                if (!is_integer(++spos)) {
                    return VALKEYMODULE_ERR;
                }
                fields = tol(spos++);
                if (fields != argc - spos) {
                    return VALKEYMODULE_ERR;
                }
                fields_start = spos;
            } else {
                return VALKEYMODULE_ERR;
            }
            return VALKEYMODULE_OK;
        }
    };

    struct httl_spec : base_key_spec {
        httl_spec &operator=(ValkeyModuleString **) = delete;

        httl_spec &operator=(const httl_spec &) = delete;

        httl_spec(const hgetex_spec &) = delete;

        httl_spec(const arg_t& argv) : base_key_spec(argv) {
        }

        int64_t fields{0};
        int64_t time_val{0};
        unsigned fields_start{0};

        int parse_options() {
            unsigned spos = 2; // the hash name is the first one
            if (argc < 3) {
                return VALKEYMODULE_OK;
            }

            if (has("fields", spos)) {
                if (!is_integer(++spos)) {
                    return VALKEYMODULE_ERR;
                }
                fields = tol(spos++);
                if (fields != argc - spos) {
                    return VALKEYMODULE_ERR;
                }
                fields_start = spos;
            } else {
                return VALKEYMODULE_ERR;
            }
            return VALKEYMODULE_OK;
        }
    };

    struct zadd_spec : base_key_spec {
        zadd_spec &operator=(ValkeyModuleString **) = delete;

        zadd_spec &operator=(const zadd_spec &) = delete;

        zadd_spec(const zadd_spec &) = delete;

        zadd_spec(const arg_t& argv) : base_key_spec(argv){

        }

        bool NX{false};
        bool XX{false};
        bool GT{false};
        bool LT{false};
        bool CH{false};
        bool INCR{false};
        bool LFI{true};

        int64_t which_flag_n{3};
        int64_t which_flag_g{3};

        unsigned fields_start{0};

        int parse_options() {
            unsigned spos = 2; // the key is the first one
            if (argc < 3) {
                return VALKEYMODULE_ERR;
            }
            // redis takes these in any order before the first score
            while (spos < argc) {
                if (has("nx", spos)) NX = true;
                else if (has("xx", spos)) XX = true;
                else if (has("gt", spos)) GT = true;
                else if (has("lt", spos)) LT = true;
                else if (has("ch", spos)) CH = true;
                else if (has("incr", spos)) INCR = true;
                else if (has("lfi", spos)) LFI = true;
                else break;
                ++spos;
            }
            if (NX && XX) return VALKEYMODULE_ERR;
            if (GT && LT) return VALKEYMODULE_ERR;
            fields_start = spos;
            return VALKEYMODULE_OK;
        }
    };

    struct zops_spec : base_key_spec {
        zops_spec &operator=(ValkeyModuleString **) = delete;

        zops_spec &operator=(const zops_spec &) = delete;

        zops_spec(const zops_spec &) = delete;

        zops_spec(const arg_t& argv) : base_key_spec(argv) {

        }

        int64_t fields_start{0};
        size_t numkeys{0};
        heap::std_vector<std::string> keys{};
        heap::std_vector<double> weight_values{};
        /** ZINTERCARD stops counting here; 0 means no limit, which is also the default */
        long long limit{0};
        bool bad_limit{false};
        /** a weight that is not a float, which redis names rather than calling it syntax */
        bool bad_weight{false};
        /** numkeys was zero, which redis names rather than calling a syntax error */
        bool no_keys{false};

        enum keyword_index {
            weights = 0,
            aggregate = 1,
            withscores = 2
        };

        enum aggregate_index {
            sum = 0,
            min = 1,
            max = 2,
            avg = 3,
            agg_none = 4
        };

        bool has_withscores{false};
        aggregate_index aggr{agg_none};

        aggregate_index map_aggr(unsigned ix) {
            switch (ix) {
                case 0:
                    return sum;
                case 1:
                    return min;
                case 2:
                    return max;
            }
            return agg_none;
        }

        int parse_options() {
            unsigned spos = 1; // the key is the first one
            // three is enough - a count and one input set. Four refused `ZUNION 1 s`,
            // which redis accepts and its own tests use
            if (argc < 3) {
                return VALKEYMODULE_ERR;
            }
            if (is_integer(spos)) {
                numkeys = tol(spos++);
            } else {
                return VALKEYMODULE_ERR;
            }
            if (numkeys == 0) {
                no_keys = true;
                return VALKEYMODULE_ERR;
            }

            while (keys.size() < numkeys) {
                if (spos >= argc) {
                    return VALKEYMODULE_ERR;
                }
                keys.push_back(tos(spos++));
            }
            if (keys.size() != numkeys) {
                return VALKEYMODULE_ERR;
            }
            if (spos == argc) {
                return VALKEYMODULE_OK;
            }
            do {
                unsigned which = has_enum({"weights", "aggregate", "withscores", "limit"}, spos);
                switch (which) {
                    case 3: // LIMIT, which only ZINTERCARD takes
                        ++spos;
                        // no value at all is a plain syntax error - the keyword is simply
                        // incomplete. A value that is there but is not a count, whether a
                        // word or a negative number, gets redis's LIMIT wording
                        if (spos >= argc) {
                            return VALKEYMODULE_ERR;
                        }
                        if (!is_integer(spos)) {
                            bad_limit = true;
                            return VALKEYMODULE_ERR;
                        }
                        limit = tol(spos++);
                        if (limit < 0) {
                            bad_limit = true;
                            return VALKEYMODULE_ERR;
                        }
                        break;
                    case weights:
                        ++spos;
                        if (!weight_values.empty()) {
                            return VALKEYMODULE_ERR;
                        }
                        // one weight per input, no more and no fewer, and each is any
                        // float rather than only an integer. Redis will not read WEIGHTS
                        // at all unless a weight for every input follows it, so a short
                        // list is a plain syntax error; a long one leaves its extras to
                        // be read as options, which fails the same way. Only a value in
                        // the right place that is not a float gets named. See DONE 115
                        // and DONE 117
                        if ((size_t) spos + numkeys > (size_t) argc) {
                            return VALKEYMODULE_ERR;
                        }
                        for (size_t n = 0; n < numkeys; ++n) {
                            double w = 0;
                            if (!to_double(spos, w)) {
                                bad_weight = true;
                                return VALKEYMODULE_ERR;
                            }
                            weight_values.push_back(w);
                            ++spos;
                        }
                        break;
                    case aggregate:
                        if (aggr != agg_none) {
                            return VALKEYMODULE_ERR;
                        }
                        aggr = map_aggr(has_enum({"sum", "min", "max", "avg"}, ++spos));
                        ++spos;
                        break;
                    case withscores:
                        if (has_withscores) {
                            return VALKEYMODULE_ERR;
                        }
                        has_withscores = true;
                        ++spos;
                        break;
                    default:
                        return VALKEYMODULE_ERR;
                }
            } while (spos < argc);
            // WITHSCORES with AGGREGATE used to be refused here for everything, which is
            // the rule for the STORE forms only - they have nowhere to put scores in a
            // reply. ZUNION, ZINTER and ZDIFF take both, and the caller that has a
            // destination is the one that knows to refuse it
            return VALKEYMODULE_OK;
        }
    };

    struct zrange_spec : base_key_spec {
        zrange_spec &operator=(ValkeyModuleString **) = delete;

        zrange_spec &operator=(const zrange_spec &) = delete;

        zrange_spec(const zops_spec &) = delete;

        zrange_spec(const arg_t& argv) : base_key_spec(argv) {
        }

        int64_t fields_start{0};
        size_t numkeys{0};
        unsigned key_pos{1};
        std::string key{};
        std::string start{};
        std::string stop{};

        enum keyword_index {
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
        bool CARD{false};
        bool has_withscores{false};
        bool has_limit{false};
        int64_t offset{0};
        int64_t count{0};

        int parse_options() {
            unsigned spos = key_pos; // the key; ZRANGESTORE puts dest in front
            if (argc < key_pos + 3) {
                return VALKEYMODULE_ERR;
            }
            key = tos(spos++);
            if (spos == argc) {
                return VALKEYMODULE_ERR;
            }
            start = tos(spos++);
            if (spos == argc) {
                return VALKEYMODULE_ERR;
            }
            stop = tos(spos++);
            if (spos == argc) {
                return VALKEYMODULE_OK;
            }

            do {
                unsigned which = has_enum({"byscore", "bylex", "rev", "limit", "withscores"}, spos);
                switch (which) {
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
                        if (!is_integer(spos)) {
                            return VALKEYMODULE_ERR;
                        }
                        offset = tol(spos++);
                        if (!is_integer(spos)) {
                            return VALKEYMODULE_ERR;
                        }
                        count = tol(spos++);
                        has_limit = true;
                        break;
                    case rev:
                        ++spos;
                        REV = true;
                        break;
                    case withscores:
                        if (has_withscores) {
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
    struct acl_spec : base_key_spec {
        acl_spec &operator=(ValkeyModuleString **) = delete;

        acl_spec &operator=(const zrange_spec &) = delete;

        acl_spec(const acl_spec &) = delete;

        acl_spec(const arg_t& argv) : base_key_spec(argv) {
        }
        enum {
            cmd_set = 0,
            cmd_get = 1,
            cmd_del = 2,
            cmd_users = 3,
            cmd_reset = 4,
            cmd_help = 5,
            cmd_count = 6
        };
        enum {
            flag_filter = 0,
            flag_cat_add = 1,
            flag_cat_rem = 2,
            flag_secret = 3,
            flag_none = 4
        };
        bool set = false;
        bool get = false;
        bool del = false;
        bool users = false;
        bool reset = false;
        bool count = false;
        bool is_filter = false;
        bool is_cat = false;
        bool is_secret = false;
        std::string user{};
        std::string secret{};
        heap::string_set filters{};
        heap::string_map<bool> cat{};
        unsigned parse_set(unsigned spos) {

            user = tos(spos++);
            if (!has("on",spos++)) {
                return VALKEYMODULE_ERR;
            }

            while (argc != spos) {
                unsigned filter = match({"~*","+*","-*",">*"},spos);
                const char * value = toc(spos++);
                if (*value == 0x00) {
                    return VALKEYMODULE_ERR;
                }
                switch (filter) {
                    case flag_filter:
                        filters.insert(&value[1]);
                        is_filter = true;
                        break;
                    case flag_cat_add:
                    case flag_cat_rem:
                        cat[&value[1]] = *value == '+';
                        is_cat = true;
                        break;
                    case flag_secret:
                        secret = &value[1];
                        is_secret = true;
                        break;
                    default:
                        return VALKEYMODULE_ERR;
                }
            }
            return VALKEYMODULE_OK;
        }
        unsigned parse_get(unsigned spos) {

            user = tos(++spos);

            return VALKEYMODULE_OK;
        }

        int parse_options() {

            unsigned spos = 1; // the command is the first one
            if (argc < 3) {
                return VALKEYMODULE_ERR;
            }
            unsigned which = has_enum({"setuser", "getuser", "del", "users", "reset", "help", "count"}, spos);
            switch (which) {
                case cmd_set:
                    set = true;
                    ++spos;
                    return parse_set(spos);
                case cmd_get:
                    get = true;
                    return parse_get(spos);
                case cmd_del:
                    del = true;
                    return parse_get(spos);
                case cmd_users:
                    users = true;
                    break;
                case cmd_reset:
                    reset = true;
                    break;
                case cmd_help:
                    break;
                case cmd_count:
                    count = true;
                    break;
                default:
                    return VALKEYMODULE_ERR;
            }
            return VALKEYMODULE_OK;
        }
    };

    struct scan_spec : base_key_spec {
        scan_spec &operator=(ValkeyModuleString **) = delete;

        scan_spec &operator=(const scan_spec &) = delete;

        scan_spec(const scan_spec &) = delete;

        scan_spec(const arg_t& argv) : base_key_spec(argv) {
        }

        unsigned fields_start{0};
        uint64_t scan_id{0};
        uint64_t count{128};
        std::string glob_expr{};
        std::string type_expr{};
        bool is_count{false};
        bool is_match{false};
        bool is_type{false};
        int parse_options() {
            unsigned spos = 1; // the command is the first one

            scan_id = toul(spos++);
            if (syntax_error) {
                return VALKEYMODULE_ERR;
            }

            if (has_one({"match","m"},spos)) {
                ++spos;
                is_match = true;
                glob_expr = tos(spos++);
                if (syntax_error) {
                    return VALKEYMODULE_ERR;
                }
            }

            if (has_one({"count","c"},spos)) {
                ++spos;
                is_count = true;
                count = toul(spos++);
                if (syntax_error) {
                    return VALKEYMODULE_ERR;
                }
            }

            if (has_one({"type","t"},spos)) {
                ++spos;
                is_type = true;
                type_expr = tos(spos++);
                if (syntax_error) {
                    return VALKEYMODULE_ERR;
                }
            }

            if (argv.size() > spos) {
                return VALKEYMODULE_ERR;
            }
            return VALKEYMODULE_OK;
        }
    };
};
#endif //SET_H
