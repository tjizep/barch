#ifndef BARCH_LOG_WORDS_CORE_H
#define BARCH_LOG_WORDS_CORE_H
/*
 * The server's own words in a Raft log - TODO 946.
 *
 * A log entry is the user, its rights, and the words of a command. The server logs
 * some entries of its own: a RELOAD's publish, a queue's push or settle, a fetch's
 * miss, the cluster's first users. Their first word starts with \x01, and the user is
 * empty. No client can call a name that starts with \x01: `rpc_caller::call` refuses
 * it. A client's entry carries its user, which is never empty.
 *
 * The seed word was written "\x01AUTHSEED". The escape takes the A as a hex digit, so
 * the word was 0x1a then "UTHSEED". A client can name that: a stored function of that
 * name, called in a Raft space, went to the log as the seed. Every member failed to
 * run it and rebuilt the space. Logs written before the fix still hold the old
 * spelling, from the server.
 *
 * This header includes no locks and no clock, and TestCoreBoundary fails if it starts
 * to. test/logwordstest.cpp runs every word against every writer.
 */
#include <string_view>

namespace barch::log_words {
    /** the seed word: the first users the cluster has. Two literals, so \x01 stays one byte */
    constexpr std::string_view seed_word = "\x01" "AUTHSEED";
    /** the seed word as logs before TODO 946 hold it */
    constexpr std::string_view old_seed_word = "\x1a" "UTHSEED";

    /** a word only the server logs: no client can call it */
    constexpr bool server_word(std::string_view w) {
        return !w.empty() && w[0] == '\x01';
    }

    /** an entry the server logged, not a client */
    constexpr bool from_server(std::string_view user) {
        return user.empty();
    }

    /**
     * Whether an entry is the seed. The old spelling only from the server: from a client
     * it is the stored function the client named. Before TODO 946 the word alone
     * decided.
     */
    constexpr bool is_seed(std::string_view word, std::string_view user) {
        return word == seed_word || (word == old_seed_word && from_server(user));
    }

    static_assert(server_word(seed_word));
    static_assert(!server_word(old_seed_word));
}

#endif
