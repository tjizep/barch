// exercising the ported queue_file: the ring, expansion, reopening, and refusal
#include "queue_file.h"
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <unistd.h>
#include <deque>
#include <fcntl.h>

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-58s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}
static std::string as_text(const std::vector<uint8_t>& v) {
    return std::string(v.begin(), v.end());
}

/*
 * The test reads and patches the 32 byte file header itself - TODO 363.
 *
 * That is deliberate rather than lazy. Placing an element so that its four byte
 * length header straddles the end of the ring means knowing where the tail is,
 * and the header is where that lives. Patching it is how a crash is simulated
 * without one: the format's claim is that an addition writes the element first
 * and the header last, so restoring an old header is exactly the state a crash
 * in between leaves behind.
 *
 * Layout, big endian, as written by queue_file::write_header:
 *   0  4  0x80000001 - the leading bit says versioned, the rest is version 1
 *   4  8  file length
 *   12 4  element count
 *   16 8  position of the first element
 *   24 8  position of the last element
 */
struct head_view {
    uint64_t file_length{0};
    uint32_t count{0};
    uint64_t first{0};
    uint64_t last{0};
};

static uint32_t be32(const uint8_t* b) {
    return ((uint32_t) b[0] << 24) | ((uint32_t) b[1] << 16) | ((uint32_t) b[2] << 8) | b[3];
}
static uint64_t be64(const uint8_t* b) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | b[i];
    return v;
}
static void put_be32(uint8_t* b, uint32_t v) {
    for (int i = 3; i >= 0; --i) { b[i] = (uint8_t) (v & 0xFF); v >>= 8; }
}
static void put_be64(uint8_t* b, uint64_t v) {
    for (int i = 7; i >= 0; --i) { b[i] = (uint8_t) (v & 0xFF); v >>= 8; }
}

static void read_bytes(const std::string& path, uint64_t at, uint8_t* into, size_t n) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::perror("open for reading"); return; }
    if (::pread(fd, into, n, (off_t) at) != (ssize_t) n) std::perror("pread");
    ::close(fd);
}
static void write_bytes(const std::string& path, uint64_t at, const uint8_t* from, size_t n) {
    const int fd = ::open(path.c_str(), O_WRONLY);
    if (fd < 0) { std::perror("open for writing"); return; }
    if (::pwrite(fd, from, n, (off_t) at) != (ssize_t) n) std::perror("pwrite");
    ::fsync(fd);
    ::close(fd);
}

/** the file header as it stands, read straight out of the file */
static head_view read_head(const std::string& path) {
    uint8_t b[32]{};
    read_bytes(path, 0, b, sizeof b);
    return head_view{be64(b + 4), be32(b + 12), be64(b + 16), be64(b + 24)};
}
static void write_head(const std::string& path, const head_view& h) {
    uint8_t b[32]{};
    put_be32(b, 0x80000001u);
    put_be64(b + 4, h.file_length);
    put_be32(b + 12, h.count);
    put_be64(b + 16, h.first);
    put_be64(b + 24, h.last);
    write_bytes(path, 0, b, sizeof b);
}

/** did opening it throw, and what did it say */
static bool refuses(const std::string& path, std::string& why) {
    try {
        barch::queue_file q(path);
    } catch (const std::exception& e) {
        why = e.what();
        return true;
    }
    why.clear();
    return false;
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

    std::printf("an element straddling the end of the ring\n");
    {
        /*
         * The wrap case above adds and removes and then checks that fifteen
         * elements come back at the right length. That is not the interesting
         * part - TODO 363. What ring_read and ring_write exist for is an element
         * that is split by the end of the file, and there are two kinds:
         *
         *   - its data is split, its four byte length header is not;
         *   - its length header itself is split, so even reading how long the
         *     element is takes two reads.
         *
         * Both are placed on purpose here rather than hoped for. The tail walks
         * by a fixed stride with every add-and-remove, the test reads the file
         * header to see where the tail is, and it stops when the tail is where
         * it wants it. 17 byte payloads mean a stride of 21, which is coprime
         * with the 4064 byte ring, so every position is reached eventually.
         */
        ::unlink(path.c_str());
        const uint32_t payload = 17;
        barch::queue_file q(path, {barch::sync_when::never, 0});
        std::deque<std::string> expected;                 // what should be in there
        uint64_t n = 0;
        const auto make = [&](uint64_t i) {
            std::string v = "e" + std::to_string(i);
            v.resize(payload, '.');
            return v;
        };
        for (int i = 0; i < 10; ++i, ++n) {
            const auto v = make(n);
            q.add(v);
            expected.push_back(v);
        }
        const uint64_t ring_end = q.file_bytes();

        // exact contents, through for_each and through peek
        const auto contents_are_right = [&]() {
            size_t at = 0;
            bool ok = true;
            q.for_each([&](const uint8_t* d, uint32_t len) {
                if (at >= expected.size() || len != expected[at].size()
                    || std::memcmp(d, expected[at].data(), len) != 0)
                    ok = false;
                ++at;
                return true;
            });
            if (at != expected.size())
                ok = false;
            std::vector<uint8_t> head;
            if (!q.peek(head) || as_text(head) != expected.front())
                ok = false;
            return ok;
        };

        int data_split = 0, header_split = 0;
        bool data_read_back = false, header_read_back = false;
        uint64_t header_split_at = 0;
        for (int round = 0; round < 6000; ++round) {
            q.remove();
            expected.pop_front();
            const auto v = make(++n);
            q.add(v);
            expected.push_back(v);

            const auto h = read_head(path);
            if (h.file_length != ring_end)
                break;                                    // it grew, which it should not
            const bool splits_header = h.last + barch::queue_file::element_header_length > h.file_length;
            const bool splits_data = !splits_header
                && h.last + barch::queue_file::element_header_length + payload > h.file_length;
            if (!splits_header && !splits_data)
                continue;

            if (splits_data) {
                ++data_split;
                if (data_split == 1)
                    data_read_back = contents_are_right();
            } else {
                ++header_split;
                if (header_split == 1) {
                    header_split_at = h.last;
                    header_read_back = contents_are_right();
                    // and a reopen, which has to read that split length header
                    // before it knows anything at all about the element
                    {
                        barch::queue_file again(path, {barch::sync_when::never, 0});
                        std::vector<uint8_t> got;
                        if (again.size() != expected.size()
                            || !again.peek(got) || as_text(got) != expected.front())
                            header_read_back = false;
                        size_t at = 0;
                        again.for_each([&](const uint8_t* d, uint32_t len) {
                            if (at >= expected.size() || len != expected[at].size()
                                || std::memcmp(d, expected[at].data(), len) != 0)
                                header_read_back = false;
                            ++at;
                            return true;
                        });
                        if (at != expected.size())
                            header_read_back = false;
                    }
                }
            }
            if (data_split && header_split)
                break;
        }

        check(q.file_bytes() == ring_end, "the ring never grew, so it really wrapped");
        check(data_split > 0, "an element's data was placed across the end of the ring");
        check(data_read_back, "and it reads back byte for byte");
        check(header_split > 0, "an element's length header was placed across the end");
        check(header_read_back, "and it reads back, including after a reopen");
        if (header_split_at)
            std::printf("    (the split length header sat at %llu of %llu)\n",
                        (unsigned long long) header_split_at, (unsigned long long) ring_end);

        // and the destructive operations, while it is still wrapped
        // still the split-header element at the tail, so this exercises remove
        // and clear against an element the end of the file runs through
        const auto h = read_head(path);
        check(h.last + barch::queue_file::element_header_length > h.file_length
              || h.last < h.first,
              "the end of the file still runs through the last element");
        q.remove(4);
        for (int i = 0; i < 4; ++i) expected.pop_front();
        check(q.size() == expected.size(), "remove(n) works while wrapped");
        check(contents_are_right(), "and leaves the rest exactly as they were");
        q.clear();
        check(q.empty() && q.file_bytes() == barch::queue_file::initial_length,
              "clear() while wrapped empties it and truncates back");
    }

    std::printf("a wrapped queue that zeroes what it removes\n");
    {
        // the same wrap, with zero_removed on, so ring_erase has to wrap too
        ::unlink(path.c_str());
        barch::queue_file q(path, {barch::sync_when::never, 0}, true);
        const std::string blob(60, 'w');
        for (int i = 0; i < 30; ++i) q.add(blob + std::to_string(i));
        const uint64_t before = q.file_bytes();
        bool wrapped = false;
        for (int round = 0; round < 400; ++round) {
            q.remove();
            q.add(blob + std::to_string(round));
            const auto h = read_head(path);
            if (h.last < h.first) wrapped = true;
        }
        check(wrapped, "the ring wrapped with zero_removed on");
        check(q.file_bytes() == before, "and did not grow");
        check(q.size() == 30, "thirty elements still");
        bool ok = true;
        q.for_each([&](const uint8_t* d, uint32_t len) {
            if (len < blob.size() || std::memcmp(d, blob.data(), blob.size()) != 0) ok = false;
            return true;
        });
        check(ok, "and every one of them still reads whole");
    }

    std::printf("truncate keeps the eldest - TODO 466\n");
    {
        ::unlink(path.c_str());
        {
            barch::queue_file q(path);
            for (int i = 0; i < 10; ++i) q.add("e" + std::to_string(i));
            q.truncate(10);
            check(q.size() == 10, "truncate to its own size changes nothing");
            q.truncate(4);
            check(q.size() == 4, "truncate(4) leaves four");
            q.add(std::string("after"));
        }
        barch::queue_file q(path);
        std::vector<std::string> seen;
        q.for_each([&](const uint8_t* d, uint32_t n) {
            seen.emplace_back((const char*) d, n);
            return true;
        });
        check(seen.size() == 5 && seen[0] == "e0" && seen[3] == "e3" && seen[4] == "after",
              "the eldest four survive a reopen, then what was added after");
        q.truncate(0);
        check(q.empty() && q.file_bytes() == barch::queue_file::initial_length,
              "truncate(0) is a clear");
    }

    std::printf("truncate on a wrapped ring that zeroes - TODO 466\n");
    {
        ::unlink(path.c_str());
        barch::queue_file q(path, {barch::sync_when::never, 0}, true);
        const std::string blob(60, 't');
        for (int i = 0; i < 30; ++i) q.add(blob);
        bool wrapped = false;
        int round = 0;
        // go round until the tail is behind the head, i.e. the kept part wraps
        for (; round < 400 && !wrapped; ++round) {
            q.remove();
            q.add(blob + std::to_string(round));
            const auto h = read_head(path);
            wrapped = h.last < h.first;
        }
        check(wrapped, "the ring wrapped");
        std::vector<std::string> before;
        q.for_each([&](const uint8_t* d, uint32_t n) {
            before.emplace_back((const char*) d, n);
            return true;
        });
        q.truncate(20);
        for (int i = 0; i < 5; ++i) q.add("new" + std::to_string(i));
        barch::queue_file again(path, {barch::sync_when::never, 0}, true);
        std::vector<std::string> after;
        again.for_each([&](const uint8_t* d, uint32_t n) {
            after.emplace_back((const char*) d, n);
            return true;
        });
        bool kept = after.size() == 25;
        for (size_t i = 0; kept && i < 20; ++i) kept = after[i] == before[i];
        for (size_t i = 20; kept && i < 25; ++i) kept = after[i] == "new" + std::to_string(i - 20);
        check(kept, "the eldest twenty, then the new five, across the wrap");
    }

    std::printf("a write that did not finish\n");
    {
        /*
         * The format's atomicity claim - TODO 363. An addition writes the
         * element into the ring first and the header last, so a crash in
         * between leaves the file exactly as it was, minus an element nobody
         * was told about. Restoring a snapshotted header is that state
         * precisely, and it is the only way to get at it without a real crash.
         */
        ::unlink(path.c_str());
        {
            barch::queue_file q(path, {barch::sync_when::never, 0});
            for (int i = 0; i < 6; ++i) q.add("kept " + std::to_string(i));
        }
        const head_view before = read_head(path);
        {
            barch::queue_file q(path, {barch::sync_when::never, 0});
            for (int i = 0; i < 4; ++i) q.add("lost " + std::to_string(i));
            check(q.size() == 10, "ten elements before the header is put back");
        }
        write_head(path, before);                    // the crash: the header never landed

        barch::queue_file q(path, {barch::sync_when::never, 0});
        check(q.size() == 6, "the six that were committed are what comes back");
        bool only_kept = true;
        int seen = 0;
        q.for_each([&](const uint8_t* d, uint32_t len) {
            if (std::string((const char*) d, len) != "kept " + std::to_string(seen))
                only_kept = false;
            ++seen;
            return true;
        });
        check(seen == 6 && only_kept, "in order, and none of the four is visible");
        // and it is not just readable, it still works
        q.add(std::string("after the crash"));
        std::vector<uint8_t> got;
        check(q.size() == 7, "and it can still be added to");
        q.remove(6);
        check(q.peek(got) && as_text(got) == "after the crash",
              "the new element is where it should be");
    }

    std::printf("a file truncated in more than one way\n");
    {
        const auto forty = [&]() {
            ::unlink(path.c_str());
            barch::queue_file q(path, {barch::sync_when::never, 0});
            for (int i = 0; i < 40; ++i) q.add(std::string(100, 'z'));
            return q.file_bytes();
        };
        std::string why;

        const uint64_t full = forty();
        if (::truncate(path.c_str(), (off_t) (full - 1)) != 0) std::perror("truncate");
        check(refuses(path, why) && why.find("truncated") != std::string::npos,
              "one byte short is refused, and says truncated");

        forty();
        if (::truncate(path.c_str(), 32) != 0) std::perror("truncate");
        check(refuses(path, why), "cut back to nothing but the header, refused");

        forty();
        if (::truncate(path.c_str(), 0) != 0) std::perror("truncate");
        check(refuses(path, why), "an empty file where a queue was, refused");

        // a header whose own numbers are nonsense - what a torn header looks like
        forty();
        head_view h = read_head(path);
        h.file_length = 8;                       // smaller than the header itself
        write_head(path, h);
        check(refuses(path, why) && why.find("corrupt") != std::string::npos,
              "a length smaller than the header is refused as corrupt");
    }

    std::printf("a header pointing at bytes that never arrived\n");
    {
        /*
         * What the durability note in queue_file.h warns about, now actually
         * tried - TODO 363. Below `each_add` nothing orders the element's bytes
         * against the header that points at them, so a crash can leave a header
         * describing an element whose bytes are not there. The first four of
         * those bytes are the element's length, so the reader can pick up a
         * wild number.
         *
         * This writes that state on purpose: garbage into free ring space, and a
         * header that counts it as an element. The claim being checked is not
         * that the data survives - it cannot - but that the reader refuses
         * instead of walking off the end or trying to allocate the garbage.
         */
        ::unlink(path.c_str());
        uint64_t tail = 0;
        {
            barch::queue_file q(path, {barch::sync_when::never, 0});
            for (int i = 0; i < 4; ++i) q.add(std::string(40, 'a'));
            const auto h = read_head(path);
            tail = h.last + barch::queue_file::element_header_length + 40;
        }
        // 0xFF bytes where the next element would go: a length of about 4 GB
        const std::vector<uint8_t> garbage(64, 0xFF);
        write_bytes(path, tail, garbage.data(), garbage.size());
        head_view h = read_head(path);
        h.count += 1;
        h.last = tail;
        write_head(path, h);

        std::string why;
        bool threw_on_open = refuses(path, why);
        bool threw_on_read = false;
        if (!threw_on_open) {
            try {
                barch::queue_file q(path, {barch::sync_when::never, 0});
                q.for_each([](const uint8_t*, uint32_t) { return true; });
            } catch (const std::exception& e) {
                threw_on_read = true;
                why = e.what();
            }
        }
        check(threw_on_open || threw_on_read,
              "a wild element length is refused rather than followed");
        std::printf("    (it said: %s)\n", why.empty() ? "nothing" : why.c_str());
    }

    ::unlink(path.c_str());
    std::printf("\n%s\n", failures == 0 ? "all queue_file checks pass"
                                        : "FAILURES: see above");
    return failures == 0 ? 0 : 1;
}
