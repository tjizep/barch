// the AOF record framing: round trips, and refusals for everything a torn
// write can produce - TODO 353
#include "aof_record.h"
#include "queue_file.h"
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <unistd.h>

using namespace barch;

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-58s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

int main() {
    std::printf("crc32c against known answers\n");
    {
        // the standard Castagnoli check values
        const std::string nine = "123456789";
        const uint32_t got = aof::crc32c((const uint8_t*) nine.data(), nine.size());
        check(got == 0xE3069283u, "\"123456789\" is 0xE3069283");
        check(aof::crc32c(nullptr, 0) == 0u, "nothing hashes to zero");
        const std::string a = "a", b = "b";
        check(aof::crc32c((const uint8_t*) a.data(), 1)
              != aof::crc32c((const uint8_t*) b.data(), 1), "different bytes differ");
    }

    std::printf("round trips\n");
    {
        std::vector<uint8_t> buf;
        aof::record in;
        in.type = aof::record_type::set;
        in.sequence = 0x0123456789ABCDEFull;
        in.expiry_ms = 1789500000000ll;
        in.space = "shop";
        in.key = "basket:42";
        in.value = std::string(1000, 'v');
        aof::encode(in, buf);

        aof::record out;
        check(aof::decode(buf.data(), (uint32_t) buf.size(), out) == aof::decoded::ok,
              "a set decodes");
        check(out.type == in.type && out.sequence == in.sequence
              && out.expiry_ms == in.expiry_ms && out.space == in.space
              && out.key == in.key && out.value == in.value, "every field survives");

        in.type = aof::record_type::erase;
        in.value.clear();
        aof::encode(in, buf);
        check(aof::decode(buf.data(), (uint32_t) buf.size(), out) == aof::decoded::ok
              && out.type == aof::record_type::erase && out.value.empty(),
              "an erase decodes with no value");

        in.type = aof::record_type::checkpoint;
        in.key.clear();
        in.space = "shop";
        aof::encode(in, buf);
        check(aof::decode(buf.data(), (uint32_t) buf.size(), out) == aof::decoded::ok
              && out.type == aof::record_type::checkpoint && out.key.empty(),
              "a checkpoint decodes with no key");

        // the options byte, which is what makes a replay repeatable
        in.type = aof::record_type::set;
        in.space = "shop"; in.key = "ck"; in.value = "compressed-bytes";
        in.options = 16 | 8 | 2;            // compressed, hashed, volatile
        aof::encode(in, buf);
        check(aof::decode(buf.data(), (uint32_t) buf.size(), out) == aof::decoded::ok
              && out.options == in.options,
              "the key option flags survive (compressed|hashed|volatile)");
        in.options = 0;

        in.space.clear(); in.key.clear(); in.value.clear();
        aof::encode(in, buf);
        check(buf.size() == aof::header_length, "an empty record is just the header");
        check(aof::decode(buf.data(), (uint32_t) buf.size(), out) == aof::decoded::ok,
              "and still decodes");
    }

    std::printf("refusals\n");
    {
        aof::record in;
        in.type = aof::record_type::set;
        in.space = "shop"; in.key = "k"; in.value = "value";
        std::vector<uint8_t> good;
        aof::encode(in, good);
        aof::record out;

        // a flipped bit anywhere at all
        int caught = 0, tried = 0;
        for (size_t byte = 0; byte < good.size(); ++byte) {
            for (int bit = 0; bit < 8; ++bit) {
                std::vector<uint8_t> bad = good;
                bad[byte] ^= (uint8_t) (1u << bit);
                ++tried;
                if (aof::decode(bad.data(), (uint32_t) bad.size(), out) != aof::decoded::ok)
                    ++caught;
            }
        }
        check(caught == tried, "every single bit flip is refused ("
              + std::to_string(caught) + "/" + std::to_string(tried) + ")");

        std::vector<uint8_t> shortened(good.begin(), good.end() - 1);
        check(aof::decode(shortened.data(), (uint32_t) shortened.size(), out)
              == aof::decoded::bad_framing, "a truncated record is refused");
        check(aof::decode(good.data(), aof::header_length - 1, out)
              == aof::decoded::too_short, "fewer bytes than a header is refused");

        std::vector<uint8_t> versioned = good;
        versioned[4] = 99;
        check(aof::decode(versioned.data(), (uint32_t) versioned.size(), out)
              == aof::decoded::bad_version, "an unknown version is refused");

        // an unknown type, with the checksum put right so it is the type being
        // tested and not the crc
        std::vector<uint8_t> typed = good;
        typed[5] = 77;
        const uint32_t fixed = aof::crc32c(typed.data() + 4, typed.size() - 4);
        for (int i = 0; i < 4; ++i)
            typed[i] = (uint8_t) (fixed >> (8 * i));
        check(aof::decode(typed.data(), (uint32_t) typed.size(), out)
              == aof::decoded::bad_framing, "an unknown record type is refused");

        // and a length triple that does not match the element, checksum valid
        std::vector<uint8_t> framed = good;
        framed[8] = (uint8_t) (framed[8] + 1);          // key length, one too long
        const uint32_t f2 = aof::crc32c(framed.data() + 4, framed.size() - 4);
        for (int i = 0; i < 4; ++i)
            framed[i] = (uint8_t) (f2 >> (8 * i));
        check(aof::decode(framed.data(), (uint32_t) framed.size(), out)
              == aof::decoded::bad_framing, "lengths that do not add up are refused");

        check(aof::describe(aof::decoded::bad_checksum) == "checksum mismatch",
              "a refusal can say why");
    }

    std::printf("through a queue file\n");
    {
        const std::string path = "aofrecordtest.dat";
        ::unlink(path.c_str());
        {
            queue_file q(path, {sync_when::on_demand, 0});
            for (int i = 0; i < 200; ++i) {
                aof::record r;
                r.type = (i % 7 == 0) ? aof::record_type::erase : aof::record_type::set;
                r.sequence = (uint64_t) i;
                r.space = "shop";
                r.key = "k" + std::to_string(i);
                r.value = (r.type == aof::record_type::set)
                    ? std::string((size_t) (i % 90) + 1, 'x') : std::string();
                std::vector<uint8_t> buf;
                aof::encode(r, buf);
                q.add(buf);
            }
            q.sync();
        }
        queue_file q(path);
        uint64_t expect_seq = 0;
        int decoded_ok = 0;
        bool ordered = true;
        q.for_each([&](const uint8_t* d, uint32_t n) {
            aof::record r;
            if (aof::decode(d, n, r) != aof::decoded::ok)
                return false;
            if (r.sequence != expect_seq) ordered = false;
            ++expect_seq;
            ++decoded_ok;
            return true;
        });
        check(decoded_ok == 200, "200 records survive a queue file round trip");
        check(ordered, "in the order they were written");
        ::unlink(path.c_str());
    }

    std::printf("\n%s\n", failures == 0 ? "all aof record checks pass" : "FAILURES above");
    return failures == 0 ? 0 : 1;
}
