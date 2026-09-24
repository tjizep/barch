//
// INDEX - secondary indexes over the fields of composite keys, TODO 422 phase 1.
//
// An index is over the composite keys of the connection's space that have `fields`
// components and then `pinned` more, which stay last and untouched (the doc id of an
// n-gram key). It is held as chains - orderings of the fields - in `<space>_ix`, and
// a chain is only there once it's built. See perm_index.h for why C(n, n/2) chains
// cover every set of fields.
//
//   INDEX CREATE <name> COMPOSITE <fields> [PINNED <n>]
//   INDEX LIST                        a line per index
//   INDEX CHAINS <name>               a line per chain: its number, whether it's
//                                     built, and its order of fields
//   INDEX BUILD <name> <chain>|ALL    write the chain's paths for the keys there now
//   INDEX FIND <name> [<field>=<value> ...] [LIMIT <n>]
//                                     the keys whose fields equal those values,
//                                     through the chain that has exactly them in front
//   INDEX DROP <name>                 the definition and every path
//
// Phase 1: a build covers the keys that exist when it runs. Writes after it aren't
// followed yet; that is phase 2's queue.
//
#include "index_api.h"

#include "caller.h"
#include "perm_index.h"

#include <cstdlib>

namespace {

std::string index_as_text(art::value_type v) {
    return {v.chars(), v.size};
}

std::string index_upper(std::string s) {
    for (auto& c : s)
        c = (char) toupper((unsigned char) c);
    return s;
}

bool index_number(const std::string& s, uint64_t& out) {
    if (s.empty() || s.size() > 19 || s.find_first_not_of("0123456789") != std::string::npos)
        return false;
    out = strtoull(s.c_str(), nullptr, 10);
    return true;
}

std::string index_line(const barch::pindex::definition& d) {
    const auto& cs = barch::pindex::chains_for(d.fields);
    std::string ready;
    for (size_t c : d.ready) {
        if (!ready.empty()) ready += ",";
        ready += std::to_string(c);
    }
    return d.name + " composite fields=" + std::to_string(d.fields) +
           " pinned=" + std::to_string(d.pinned) +
           " chains=" + std::to_string(cs.chains.size()) +
           " built=" + (ready.empty() ? "-" : ready);
}

} // namespace

int INDEX(caller& call, const arg_t& argv) {
    if (argv.size() < 2)
        return call.wrong_arity();
    const std::string sub = index_upper(index_as_text(argv[1]));
    auto space = call.kspace();
    std::string err;

    if (sub == "CREATE") {
        // INDEX CREATE name COMPOSITE fields [PINNED n]
        if (argv.size() != 5 && argv.size() != 7)
            return call.wrong_arity();
        if (index_upper(index_as_text(argv[3])) != "COMPOSITE")
            return call.push_error("INDEX CREATE name COMPOSITE fields [PINNED n]");
        barch::pindex::definition d;
        d.name = index_as_text(argv[2]);
        uint64_t fields = 0, pinned = 0;
        if (!index_number(index_as_text(argv[4]), fields))
            return call.push_error("fields is a number from 1 to 8");
        if (argv.size() == 7) {
            if (index_upper(index_as_text(argv[5])) != "PINNED" ||
                !index_number(index_as_text(argv[6]), pinned))
                return call.push_error("INDEX CREATE name COMPOSITE fields [PINNED n]");
        }
        d.fields = (size_t) fields;
        d.pinned = (size_t) pinned;
        if (!barch::pindex::create(space, d, err))
            return call.push_error(err.c_str());
        return call.push_simple("OK");
    }
    if (sub == "LIST") {
        if (argv.size() != 2)
            return call.wrong_arity();
        call.start_array();
        for (const auto& d : barch::pindex::list(space))
            call.push_string(index_line(d));
        return call.end_array();
    }
    if (sub == "CHAINS") {
        if (argv.size() != 3)
            return call.wrong_arity();
        barch::pindex::definition d;
        if (!barch::pindex::load(space, index_as_text(argv[2]), d, err))
            return call.push_error(err.c_str());
        const auto& cs = barch::pindex::chains_for(d.fields);
        call.start_array();
        for (size_t c = 0; c < cs.chains.size(); ++c) {
            std::string line = std::to_string(c) + (d.is_ready(c) ? " built" : " not-built");
            for (uint8_t f : cs.chains[c])
                line += " " + std::to_string(f);
            call.push_string(line);
        }
        return call.end_array();
    }
    if (sub == "BUILD") {
        if (argv.size() != 4)
            return call.wrong_arity();
        barch::pindex::definition d;
        if (!barch::pindex::load(space, index_as_text(argv[2]), d, err))
            return call.push_error(err.c_str());
        const auto& cs = barch::pindex::chains_for(d.fields);
        std::string which = index_upper(index_as_text(argv[3]));
        std::vector<size_t> chains;
        if (which == "ALL") {
            for (size_t c = 0; c < cs.chains.size(); ++c)
                chains.push_back(c);
        } else {
            uint64_t c = 0;
            if (!index_number(which, c))
                return call.push_error("INDEX BUILD name chain|ALL");
            chains.push_back((size_t) c);
        }
        barch::pindex::build_result total;
        for (size_t c : chains) {
            barch::pindex::build_result r;
            if (!barch::pindex::build(space, d, c, r, err))
                return call.push_error(err.c_str());
            total.records += r.records;
            total.skipped = r.skipped;          // the same keys each time
        }
        return call.push_string("records=" + std::to_string(total.records) +
                                " skipped=" + std::to_string(total.skipped));
    }
    if (sub == "FIND") {
        // INDEX FIND name [field=value ...] [LIMIT n]
        if (argv.size() < 3)
            return call.wrong_arity();
        barch::pindex::definition d;
        if (!barch::pindex::load(space, index_as_text(argv[2]), d, err))
            return call.push_error(err.c_str());
        std::vector<std::pair<size_t, std::string>> equal;
        int64_t limit = -1;
        for (size_t at = 3; at < argv.size(); ++at) {
            std::string word = index_as_text(argv[at]);
            if (index_upper(word) == "LIMIT" && at + 1 < argv.size()) {
                uint64_t n = 0;
                if (!index_number(index_as_text(argv[at + 1]), n))
                    return call.push_error("LIMIT is a whole number");
                limit = (int64_t) n;
                ++at;
                continue;
            }
            auto eq = word.find('=');
            uint64_t field = 0;
            if (eq == std::string::npos || !index_number(word.substr(0, eq), field))
                return call.push_error("INDEX FIND name [field=value ...] [LIMIT n]");
            equal.emplace_back((size_t) field, word.substr(eq + 1));
        }
        std::vector<std::string> keys;
        if (!barch::pindex::find(space, d, equal, limit, keys, err))
            return call.push_error(err.c_str());
        call.start_array();
        for (const auto& k : keys)
            call.push_encoded_key(art::value_type{k.data(), k.size()});
        return call.end_array();
    }
    if (sub == "DROP") {
        if (argv.size() != 3)
            return call.wrong_arity();
        if (!barch::pindex::drop(space, index_as_text(argv[2]), err))
            return call.push_error(err.c_str());
        return call.push_simple("OK");
    }
    return call.push_error("INDEX CREATE|LIST|CHAINS|BUILD|FIND|DROP");
}

void register_index_api(function_map& r) {
    // not "data": an index is derived from the space, and a replica builds its own
    r["INDEX"] = {::INDEX, {"read", "write", "keys"}};
}
