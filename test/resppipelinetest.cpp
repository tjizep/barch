//
// Requests pipelined across reads - TODO 589.
//
// The shop's catalog load sends 7385 SETs through `redis-cli --pipe`, values up to
// 65 KB, and barchd answered "invalid array size" partway through, while the same
// commands sent one at a time all worked. So the request parser lost its place
// somewhere between reads.
//
// This feeds a stream like that into redis_parser the way asio_resp_session does:
// add_data per read, then read_new_request until it comes back empty, sometimes
// stopping early with data left over, the way back pressure does (TODO 490). Every
// request has to come out exactly as it went in, for several read sizes.
//
// With a file argument it replays that file instead and says where it fails, which
// is how the captured catalog load was looked at.
//
#include "rpc/redis_parser.h"

#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <exception>
#include <fstream>
#include <iterator>
#include <random>
#include <string>
#include <vector>

namespace {
    using request = std::vector<std::string>;

    std::string encode(const request& r) {
        std::string out = "*" + std::to_string(r.size()) + "\r\n";
        for (const auto& a : r)
            out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
        return out;
    }

    /** what the catalog load looks like: SET key value, values of every size */
    std::vector<request> catalog_like(uint32_t seed, size_t count) {
        std::mt19937 rng(seed);
        std::vector<request> out;
        out.push_back({"USE", "inventory"});
        const std::string alphabet = "{}[]\":,abcdefghijklmnopqrstuvwxyz0123456789 ";
        for (size_t i = 0; i < count; ++i) {
            // mostly a few hundred bytes, some tens of KB, like p:<asin> and index:<n>
            size_t len = (rng() % 8 == 0) ? 20000 + rng() % 50000 : 200 + rng() % 1500;
            std::string v(len, 'x');
            for (auto& c : v)
                c = alphabet[rng() % alphabet.size()];
            // binary safe: a value may hold CR and LF, and the parser must not care
            if (getenv("NOCRLF") == nullptr && rng() % 16 == 0)
                v.insert(rng() % v.size(), "\r\n");
            out.push_back({"SET", "p:" + std::to_string(i), v});
        }
        return out;
    }

    /**
     * Feed `wire` in reads of `chunk` bytes (0: varied), parsing after each like
     * consume_available. Answers how many requests came out, and fills `err` on
     * the first one that is wrong or throws.
     */
    size_t replay(const std::string& wire, const std::vector<request>* expect, size_t chunk,
                  uint32_t seed, std::string& err) {
        redis::redis_parser parser;
        std::mt19937 rng(seed);
        size_t at = 0, got = 0;
        while (at < wire.size() || parser.remaining() > 0) {
            if (at < wire.size()) {
                size_t n = chunk ? chunk : 1 + rng() % 70000;
                if (n > wire.size() - at)
                    n = wire.size() - at;
                parser.add_data(wire.data() + at, n);
                at += n;
            }
            // back pressure: sometimes only part of what is buffered is parsed
            // before the next read arrives
            const bool limited = rng() % 4 == 0;
            size_t budget = limited ? 1 + rng() % 3 : SIZE_MAX;
            try {
                while (parser.remaining() > 0 && budget > 0) {
                    --budget;
                    const auto& params = parser.read_new_request();
                    if (params.empty())
                        break;
                    if (expect) {
                        if (got >= expect->size()) {
                            err = "more requests than were sent";
                            return got;
                        }
                        const auto& want = (*expect)[got];
                        bool same = params.size() == want.size();
                        for (size_t i = 0; same && i < want.size(); ++i)
                            same = std::string(params[i]) == want[i];
                        if (!same) {
                            err = "request " + std::to_string(got) + " came out different";
                            return got;
                        }
                    }
                    ++got;
                }
            } catch (const std::exception& e) {
                err = "request " + std::to_string(got) + ", after " + std::to_string(at) +
                      " bytes read: " + e.what();
                return got;
            }
            if (at >= wire.size() && limited && parser.remaining() > 0)
                continue;                   // still draining what was left
            if (at >= wire.size())
                break;
        }
        return got;
    }
}

int main(int argc, char** argv) {
    if (argc > 1) {
        std::ifstream in(argv[1], std::ios::binary);
        std::string wire((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        int bad = 0;
        for (size_t chunk : {size_t(16384), size_t(65536), size_t(4096), size_t(0)}) {
            std::string err;
            auto n = replay(wire, nullptr, chunk, 7, err);
            std::printf("%s: chunk %zu: %zu requests%s%s\n", argv[1], chunk, n,
                        err.empty() ? "" : ", ", err.c_str());
            bad += !err.empty();
        }
        return bad ? 1 : 0;
    }

    auto reqs = catalog_like(42, 3000);
    std::string wire;
    for (const auto& r : reqs)
        wire += encode(r);
    int failures = 0;
    for (size_t chunk : {size_t(16384), size_t(65536), size_t(4096), size_t(1), size_t(0)}) {
        for (uint32_t seed : {1u, 2u, 3u}) {
            std::string err;
            auto n = replay(wire, &reqs, chunk, seed, err);
            if (!err.empty() || n != reqs.size()) {
                std::printf("FAIL: chunk %zu seed %u: %zu of %zu requests%s%s\n", chunk, seed,
                            n, reqs.size(), err.empty() ? "" : ", ", err.c_str());
                ++failures;
            }
            if (chunk == 1)
                break;                      // byte at a time is slow; once is enough
        }
    }
    /*
     * An empty line between requests. redis-cli --pipe sends one right before the
     * ECHO it closes with, and redis skips it like an empty inline command; the
     * parser took it as part of the next header and answered "invalid array size",
     * which is what stopped the shop's catalog load. Also split across reads, CR in
     * one and LF in the next.
     */
    {
        std::vector<request> two = {{"SET", "a", "1"}, {"ECHO", "end"}};
        std::string with_blank = encode(two[0]) + "\r\n" + encode(two[1]);
        std::string with_blanks = encode(two[0]) + "\r\n\r\n" + encode(two[1]);
        for (const auto* wire2 : {&with_blank, &with_blanks}) {
            for (size_t chunk : {wire2->size(), size_t(1), size_t(3)}) {
                std::string err;
                auto n = replay(*wire2, &two, chunk, 5, err);
                if (!err.empty() || n != two.size()) {
                    std::printf("FAIL: an empty line between requests, chunk %zu: %zu of 2%s%s\n",
                                chunk, n, err.empty() ? "" : ", ", err.c_str());
                    ++failures;
                }
            }
        }
    }

    if (failures) {
        std::printf("%d replay(s) failed\n", failures);
        return 1;
    }
    std::printf("resp pipeline: ok, %zu requests, %zu bytes\n", reqs.size(), wire.size());
    return 0;
}
