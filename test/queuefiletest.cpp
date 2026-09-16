// exercising the ported queue_file: the ring, expansion, reopening, and refusal
#include "queue_file.h"
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <unistd.h>

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-58s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}
static std::string as_text(const std::vector<uint8_t>& v) {
    return std::string(v.begin(), v.end());
}

int main() {
    // the ctest working directory, so a failed run leaves the file to look at
    const std::string path = "queuefiletest.dat";
    ::unlink(path.c_str());

    std::printf("a new file\n");
    {
        barch::queue_file q(path);
        check(q.empty() && q.size() == 0, "starts empty");
        check(q.file_bytes() == barch::queue_file::initial_length, "starts at 4096 bytes");
        check(q.is_versioned(), "uses the 32 byte header");
        std::vector<uint8_t> got;
        check(!q.peek(got), "peek on an empty queue says so");
    }

    std::printf("fifo order and removal\n");
    {
        barch::queue_file q(path);
        q.add(std::string("one"));
        q.add(std::string("two"));
        q.add(std::string("three"));
        check(q.size() == 3, "three elements");
        std::vector<uint8_t> got;
        check(q.peek(got) && as_text(got) == "one", "peek gives the eldest");
        q.remove();
        check(q.peek(got) && as_text(got) == "two", "remove drops the eldest");
        q.remove(2);
        check(q.empty(), "removing the rest empties it");
        check(q.file_bytes() == barch::queue_file::initial_length, "and truncates back");
    }

    std::printf("survives being closed and reopened\n");
    {
        {
            barch::queue_file q(path);
            for (int i = 0; i < 50; ++i)
                q.add("element " + std::to_string(i));
        }
        barch::queue_file q(path);
        check(q.size() == 50, "the elements are still there");
        int seen = 0;
        bool ordered = true;
        q.for_each([&](const uint8_t* d, uint32_t n) {
            if (as_text(std::vector<uint8_t>(d, d + n)) != "element " + std::to_string(seen))
                ordered = false;
            ++seen;
            return true;
        });
        check(seen == 50 && ordered, "for_each walks them in order");
        int stopped = 0;
        q.for_each([&](const uint8_t*, uint32_t) { ++stopped; return stopped < 3; });
        check(stopped == 3, "for_each stops when asked");
        check(q.size() == 50, "and walking changes nothing");
    }

    std::printf("the ring wraps\n");
    {
        ::unlink(path.c_str());
        barch::queue_file q(path);
        // fill, then keep adding and removing so the head and tail cross the end
        const std::string blob(200, 'x');
        for (int i = 0; i < 15; ++i) q.add(blob);
        const uint64_t before = q.file_bytes();
        for (int round = 0; round < 40; ++round) {
            q.remove();
            q.add(blob + std::to_string(round));
        }
        check(q.file_bytes() == before, "no expansion was needed");
        check(q.size() == 15, "still fifteen elements");
        std::vector<uint8_t> got;
        check(q.peek(got) && as_text(got).size() >= 200, "the head still reads whole");
        int seen = 0;
        q.for_each([&](const uint8_t*, uint32_t n) { if (n >= 200) ++seen; return true; });
        check(seen == 15, "every element reads back at full length across the wrap");
    }

    std::printf("expansion while wrapped\n");
    {
        ::unlink(path.c_str());
        barch::queue_file q(path);
        const std::string blob(300, 'y');
        for (int i = 0; i < 10; ++i) q.add(blob);
        for (int i = 0; i < 6; ++i) { q.remove(); q.add(blob); }   // wrap the ring
        const uint64_t before = q.file_bytes();
        for (int i = 0; i < 40; ++i) q.add(blob);                  // force growth
        check(q.file_bytes() > before, "the file grew");
        check(q.size() == 50, "with every element intact");
        bool all_right = true;
        q.for_each([&](const uint8_t* d, uint32_t n) {
            if (n != blob.size() || std::memcmp(d, blob.data(), n) != 0) all_right = false;
            return true;
        });
        check(all_right, "and every element still has its exact bytes");
        {
            barch::queue_file again(path);
            check(again.size() == 50, "a reopen agrees after expansion");
        }
    }

    std::printf("a truncated file is refused\n");
    {
        ::unlink(path.c_str());
        {
            barch::queue_file q(path);
            for (int i = 0; i < 40; ++i) q.add(std::string(100, 'z'));
        }
        // lop the end off, the way an interrupted write would
        if (::truncate(path.c_str(), 2048) != 0) std::perror("truncate");
        bool threw = false;
        try {
            barch::queue_file q(path);
        } catch (const std::exception&) {
            threw = true;
        }
        check(threw, "opening it throws rather than reading wild");
    }

    ::unlink(path.c_str());
    std::printf("\n%s\n", failures == 0 ? "all queue_file checks pass"
                                        : "FAILURES: see above");
    return failures == 0 ? 0 : 1;
}
