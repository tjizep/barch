// The server's own words in a Raft log, and a client that names one - TODO 946.
//
// src/log_words_core.h decides whether a client may call a name, and whether a log
// entry is the cluster's users seed. This runs every writer against every word:
//   - the server as it is now, logging each of its words with an empty user;
//   - the server before TODO 946, logging the seed in the old spelling;
//   - a client, as each of three users, calling each of a set of names: the server's
//     words, the old spelling, names close to them, and plain commands.
// It checks:
//   - a client can't call a server word;
//   - a client's entry that the gate lets through is never the seed;
//   - every seed the server logged, in either spelling, is the seed;
//   - no other server entry is the seed.
//
// It also runs rules that are known to be wrong and checks that it catches each. One is
// the code as it was before TODO 946: the seed word alone decided.
#include "log_words_core.h"

#include <cstdio>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    using namespace barch::log_words;

    struct rules {
        std::function<bool(std::string_view)> server_word = barch::log_words::server_word;
        std::function<bool(std::string_view, std::string_view)> is_seed = barch::log_words::is_seed;
    };

    // the server's words, as src/ spells them
    const std::vector<std::string> server_words{
            std::string(seed_word), "\x01" "PUBLISH", "\x01" "QUEUE", "\x01" "MISS",
    };

    const std::vector<std::string> names{
            std::string(seed_word), std::string(old_seed_word), "\x01" "PUBLISH", "\x01" "QUEUE", "\x01" "MISS",
            "AUTHSEED", "\x1a" "UTHSEEDX", "\x01", "\x1a", "GET", "SET", "inc", "",
    };

    const std::vector<std::string> users{"default", "alice", "web"};

    struct entry {
        std::string user, word;
        bool by_server;
        bool seed;          // what the server meant
    };

    // empty if it all holds
    std::string all(const rules& r, size_t* inputs = nullptr) {
        std::vector<entry> log;
        for (const auto& w : server_words)
            log.push_back({"", w, true, w == seed_word});
        log.push_back({"", std::string(old_seed_word), true, true});
        size_t refused = 0;
        for (const auto& u : users)
            for (const auto& n : names) {
                if (!r.server_word(n)) {
                    log.push_back({u, n, false, false});
                    continue;
                }
                ++refused;
            }
        if (inputs)
            *inputs = log.size() + refused;
        for (const auto& u : users)
            for (const auto& w : server_words)
                if (!r.server_word(w))
                    return "a client can call the server word " + std::string(w == seed_word ? "seed" : w.substr(1)) +
                           " as " + u;
        for (const auto& e : log) {
            const bool seed = r.is_seed(e.word, e.user);
            if (!e.by_server && seed)
                return "a client's entry was taken for the seed";
            if (e.by_server && e.seed && !seed)
                return "a seed the server logged was not taken for the seed";
            if (e.by_server && !e.seed && seed)
                return "another server entry was taken for the seed";
        }
        return "";
    }
}

int main() {
    const rules now;

    std::printf("every writer against every word\n");
    {
        size_t inputs = 0;
        const auto got = all(now, &inputs);
        if (!got.empty())
            std::printf("  %s\n", got.c_str());
        char line[120];
        std::snprintf(line, sizeof line, "as now (%zu entries)", inputs);
        check(got.empty(), line);
    }

    std::printf("rules known to be wrong, each caught\n");
    {
        struct mutant {
            const char* what;
            rules r;
        };
        auto with = [&](auto change) {
            rules r = now;
            change(r);
            return r;
        };
        const std::vector<mutant> mutants{
                {"as before TODO 946", with([](rules& r) {
                    r.is_seed = [](std::string_view w, std::string_view) { return w == old_seed_word; };
                })},
                {"no user check", with([](rules& r) {
                    r.is_seed = [](std::string_view w, std::string_view) {
                        return w == seed_word || w == old_seed_word;
                    };
                })},
                {"the old spelling no longer read", with([](rules& r) {
                    r.is_seed = [](std::string_view w, std::string_view) { return w == seed_word; };
                })},
                {"a gate that checks the old first byte", with([](rules& r) {
                    r.server_word = [](std::string_view w) { return !w.empty() && w[0] == '\x1a'; };
                })},
        };
        for (const auto& m : mutants) {
            const auto got = all(m.r);
            char line[400];
            std::snprintf(line, sizeof line, "%s: %s", m.what, got.empty() ? "missed" : got.c_str());
            check(!got.empty(), line);
        }
    }

    std::printf(failures ? "FAILED\n" : "all passed\n");
    return failures ? 1 : 0;
}
