//
// Created by teejip on 8/10/26.
//
#include "export_api.h"

#include <fstream>
#include <iomanip>
#include <sstream>
#include <map>
#include <string>

#include "composite.h"
#include "conversion.h"
#include "keys.h"
#include "key_type.h"
#include "configuration.h"
#include "module.h"
#include "sharded_store.h"
#include "art/iterator.h"
#include "dictionary_compressor.h"
#include "rpc_caller.h"
#include "vk_caller.h"

/**
 * Why this exists, and why it is commands rather than a dump of the store.
 *
 * `storage_version` refuses a shard file written by a different build, which is the right
 * thing to do - a format that has changed should not be read as though it had not. What it
 * leaves is a user whose data is intact and unreadable. Three times today that version has
 * moved (the tplain key encoding, the per kind container leads, the clock), and each time
 * the answer to "what do I do with the data I already have" was nothing.
 *
 * So the export is deliberately not a copy of anything. It is the SET, HSET, RPUSH and
 * ZADD calls that would produce the same data on any build, in any version, through the
 * ordinary command path. That is slower and larger than a page dump and it is the only
 * form that survives the thing it exists for.
 *
 * The stream is RESP. It is binary safe, which a line based format is not - a value may
 * hold newlines, spaces, or nothing at all - and it can be replayed by anything that
 * speaks to the server, including `redis-cli --pipe`, not only by IMPORT.
 */

namespace {

    void write_bulk(std::ostream& out, const std::string& s) {
        out << "$" << s.size() << "\r\n" << s << "\r\n";
    }

    void write_command(std::ostream& out, const heap::std_vector<std::string>& args) {
        out << "*" << args.size() << "\r\n";
        for (const auto& a : args) {
            write_bulk(out, a);
        }
    }

    /** the text of a stored component, which carries its own type byte and terminator */
    std::string component_text(art::value_type component) {
        std::string framed;
        framed.push_back((char) art::tcomposite);
        framed.push_back('\x01');
        framed.append(component.chars(), component.size);
        return encoded_key_as_string(art::value_type{framed});
    }

    /** a score written so that reading it back gives the same double */
    std::string score_text(double d) {
        std::ostringstream ss;
        ss << std::setprecision(17) << d;
        return ss.str();
    }

    struct named {
        barch::container_kind kind{barch::container_kind::none};
    };

    /**
     * Every name in the space, and what it holds.
     *
     * A container's keys all carry its name in the first component, so the name is the
     * slice `encoded_container_name_len` measures and the kind is the lead byte. An
     * ordered set also keeps a member index whose first component is empty; those decode
     * to an empty name and are skipped, or every set would be exported twice.
     */
    heap::string_map<named> names_in(barch::sharded_store& store, bool& hit_ceiling) {
        // the tracking map, so what this costs is counted in get_total_memory() along with
        // everything else, and the ceiling below is measured against the real figure
        heap::string_map<named> found;
        const uint64_t memory_ceiling = barch::get_max_module_memory();
        store.each_shard_read([&](const barch::shard_ptr& t) {
            if (hit_ceiling) return;
            art::node_ptr first = t->tree_minimum();
            if (first.null() || !first.is_leaf) return;
            for (art::iterator i(t, first.const_leaf()->get_key()); i.ok(); i.next()) {
                const art::leaf *l = i.l();
                if (!l || l->is_tomb() || l->deleted() || l->expired()) continue;
                auto k = i.key();
                if (!k.size) continue;
                unsigned nl = encoded_container_name_len(k);
                if (nl) {
                    std::string name = component_text(k.sub(2, nl - 2));
                    if (name.empty()) continue;      // the ordered set's member index
                    barch::container_kind kind = barch::container_kind::none;
                    switch (*k.bytes) {
                        case art::tcomposite_list: kind = barch::container_kind::list; break;
                        case art::tcomposite_hash: kind = barch::container_kind::hash; break;
                        case art::tcomposite_ordered_map: kind = barch::container_kind::ordered_map; break;
                        default: continue;
                    }
                    found[name].kind = kind;
                    // an export that stops early and still says it worked is a backup with
                    // a hole in it, so this is a refusal rather than a short file - see the
                    // check on the result below
                    if (get_total_memory() >= memory_ceiling) {
                        hit_ceiling = true;
                        return;
                    }
                } else if (art::is_container_lead(*k.bytes)) {
                    // a container key whose name component did not decode is the ordered
                    // set's member index, whose first component is empty. Falling through
                    // to the plain branch exported it as a string key, and the import then
                    // created a key nobody had ever written
                    continue;
                } else {
                    found[encoded_key_as_string(k)].kind = barch::container_kind::none;
                    if (get_total_memory() >= memory_ceiling) {
                        hit_ceiling = true;
                        return;
                    }
                }
            }
        });
        return found;
    }

    std::string value_of(const art::leaf *l) {
        auto v = l->get_value();
        if (l->is_compressed()) {
            auto d = dictionary::decompress(v);
            return {d.chars(), d.size};
        }
        return {v.chars(), v.size};
    }

    /** the entries of one container, in the order they are stored */
    void each_entry(barch::sharded_store& store, const std::string& name,
                    barch::container_kind kind,
                    const std::function<void(art::value_type key, const art::leaf *l, size_t plen)>& cb) {
        composite q;
        art::value_type nm{name};
        art::value_type prefix = q.create(barch::lead_of(kind), {conversion::convert(nm)}, false);
        size_t plen = prefix.size;
        store.with_container_read(nm, [&](const barch::shard_ptr& t) {
            art::node_ptr lb = t->lower_bound(prefix);
            if (lb.null() || !lb.is_leaf) return;
            for (art::iterator i(t, lb.const_leaf()->get_key()); i.ok(); i.next()) {
                auto k = i.key();
                if (!k.starts_with(prefix)) return;
                const art::leaf *l = i.l();
                if (!l || l->is_tomb() || l->deleted() || l->expired()) continue;
                cb(k, l, plen);
            }
        });
    }

    size_t export_one(std::ostream& out, barch::sharded_store& store,
                      const std::string& name, barch::container_kind kind) {
        heap::std_vector<std::string> args;
        switch (kind) {
            case barch::container_kind::none: {
                auto converted = conversion::as_composite(art::value_type{name});
                std::string held;
                bool had = false;
                long long deadline = 0;
                store.with_key_read(converted.get_value(), [&](const barch::shard_ptr& t) {
                    auto n = t->search(converted.get_value());
                    if (n.null() || !n.is_leaf) return;
                    auto l = n.const_leaf();
                    held = value_of(l);
                    deadline = l->is_expiry() ? (long long) l->expiry_ms() : 0;
                    had = true;
                });
                if (!had) return 0;
                args = {"SET", name, held};
                if (deadline) {
                    // the deadline travels as an absolute time, so a slow export does not
                    // shorten it the way a remaining-seconds form would
                    args.push_back("PXAT");
                    args.push_back(std::to_string(deadline));
                }
                write_command(out, args);
                return 1;
            }
            case barch::container_kind::hash: {
                args = {"HSET", name};
                each_entry(store, name, kind, [&](art::value_type k, const art::leaf *l, size_t plen) {
                    args.push_back(component_text(k.sub(plen, k.size - plen)));
                    args.push_back(value_of(l));
                });
                if (args.size() <= 2) return 0;
                write_command(out, args);
                return 1;
            }
            case barch::container_kind::list: {
                args = {"RPUSH", name};
                each_entry(store, name, kind, [&](art::value_type k, const art::leaf *l, size_t plen) {
                    // the header sits at the prefix itself and holds the bounds rather
                    // than an element, so it is not part of the list's contents
                    if (k.size == plen) return;
                    args.push_back(value_of(l));
                });
                if (args.size() <= 2) return 0;
                write_command(out, args);
                return 1;
            }
            case barch::container_kind::ordered_map: {
                args = {"ZADD", name};
                each_entry(store, name, kind, [&](art::value_type k, const art::leaf *, size_t plen) {
                    // a score key is {name, score, member}; the member index for the same
                    // set begins with an empty component and is walked under a different
                    // prefix, so nothing here is an index entry
                    if (k.size < plen + numeric_key_size) return;
                    double score = conversion::enc_bytes_to_dbl(k.sub(plen, numeric_key_size));
                    args.push_back(score_text(score));
                    args.push_back(component_text(k.sub(plen + numeric_key_size,
                                                        k.size - plen - numeric_key_size)));
                });
                if (args.size() <= 2) return 0;
                write_command(out, args);
                return 1;
            }
        }
        return 0;
    }
}

/**
 * EXPORT <path> - write the current key space as the commands that would rebuild it.
 *
 * Answers with how many keys were written. The file is a RESP command stream: replay it
 * with IMPORT, or pipe it at any server that speaks the protocol.
 */
int EXPORT(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    std::string path(argv[1].chars(), argv[1].size);
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        return call.push_error("could not open the export file for writing");
    }
    barch::sharded_store store(call.kspace());
    size_t written = 0;
    bool hit_ceiling = false;
    auto names = names_in(store, hit_ceiling);
    if (hit_ceiling) {
        return call.push_error("not enough memory to export: raise max_memory_bytes, or "
                               "export one key space at a time");
    }
    for (const auto& [name, what] : names) {
        written += export_one(out, store, name, what.kind);
    }
    out.flush();
    if (!out) {
        return call.push_error("the export could not be written");
    }
    return call.push_ll((long long) written);
}

/**
 * IMPORT <path> - replay an export into the current key space.
 *
 * Existing keys of the same name are overwritten by whatever the stream says, and keys the
 * stream does not mention are left alone, so an import merges rather than replaces. Clear
 * the space first if a replacement is what is wanted.
 */
int IMPORT(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    std::string path(argv[1].chars(), argv[1].size);
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return call.push_error("could not open the export file for reading");
    }
    auto table = functions_by_name();
    long long applied = 0;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] != '*') continue;
        long count = strtol(line.c_str() + 1, nullptr, 10);
        if (count <= 0) continue;
        heap::std_vector<std::string> parts;
        bool complete = true;
        for (long i = 0; i < count; ++i) {
            if (!std::getline(in, line) || line.empty() || line[0] != '$') {
                complete = false;
                break;
            }
            long len = strtol(line.c_str() + 1, nullptr, 10);
            std::string arg(len < 0 ? 0 : (size_t) len, '\0');
            if (len > 0) in.read(arg.data(), len);
            // the terminator after the payload, which getline would otherwise take as a
            // line of its own
            in.get();
            in.get();
            parts.push_back(arg);
        }
        if (!complete) break;
        auto f = table->find(parts[0]);
        if (f == table->end()) {
            return call.push_error(("the export names a command this server does not have: "
                                    + parts[0]).c_str());
        }
        // replayed through a caller of its own. Handing them this one would push every
        // reply into the reply we are building, and IMPORT answers with a count
        std::vector<std::string> params(parts.begin(), parts.end());
        rpc_caller replay;
        replay.set_kspace(call.kspace());
        replay.call(params, f->second.call);
        ++applied;
    }
    return call.push_ll(applied);
}

int cmd_EXPORT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, EXPORT);
}

int cmd_IMPORT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, IMPORT);
}

void register_export_api(function_map& r) {
    r["EXPORT"] = {::EXPORT, {"read", "keys", "data", "admin"}};
    r["IMPORT"] = {::IMPORT, {"write", "keys", "data", "admin", "dangerous"}};
}
