// The named message queue: the record, at-least-once delivery, and dead lettering.
// See TODO 366.
#include "message_queue.h"

#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <unistd.h>
#include <vector>

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-60s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

int main() {
    const std::string path = "messagequeuetest.dat";
    const std::string dead = path + ".dead";
    const auto fresh = [&]() {
        ::unlink(path.c_str());
        ::unlink(dead.c_str());
    };

    std::printf("the record\n");
    {
        std::vector<uint8_t> buf;
        barch::mq::encode(42, "hello", buf);
        check(buf.size() == 16 + 5, "a five byte message is a 21 byte record");
        barch::mq::message m;
        check(barch::mq::decode(buf.data(), buf.size(), m) == barch::mq::decoded::ok,
              "it decodes");
        check(m.sequence == 42 && m.data == "hello", "with its sequence and its bytes");

        auto torn = buf;
        torn[18] ^= 0x40;                       // a bit in the payload
        check(barch::mq::decode(torn.data(), torn.size(), m) == barch::mq::decoded::bad_checksum,
              "one flipped bit in the payload is caught");
        torn = buf;
        torn[9] ^= 0x01;                        // a bit in the sequence
        check(barch::mq::decode(torn.data(), torn.size(), m) == barch::mq::decoded::bad_checksum,
              "and one in the header");
        check(barch::mq::decode(buf.data(), 8, m) == barch::mq::decoded::too_short,
              "a record shorter than its header is refused");
        auto ver = buf;
        ver[4] = 99;
        check(barch::mq::decode(ver.data(), ver.size(), m) == barch::mq::decoded::bad_version,
              "and a version this build does not know");

        std::vector<uint8_t> empty;
        barch::mq::encode(1, "", empty);
        check(barch::mq::decode(empty.data(), empty.size(), m) == barch::mq::decoded::ok
              && m.data.empty(), "an empty message is a message");
    }

    std::printf("publish and drain\n");
    {
        fresh();
        barch::mq::queue q(path, {barch::sync_when::never, 0});
        check(q.empty() && q.size() == 0, "starts empty");
        const auto a = q.publish("one");
        const auto b = q.publish("two");
        check(a == 1 && b == 2, "sequences start at one and count up");
        check(q.size() == 2, "two messages");

        barch::mq::message m;
        check(q.peek(m) && m.data == "one" && m.sequence == 1, "peek gives the eldest");
        check(m.attempts == 0, "with no attempts against it yet");
        check(q.peek(m) && m.data == "one", "peek does not take it");
        q.remove();
        check(q.peek(m) && m.data == "two", "remove moves on to the next");
        q.remove();
        check(q.empty(), "and the queue drains");
        check(!q.peek(m), "peek on an empty queue says so");
    }

    std::printf("a message outlives the process\n");
    {
        fresh();
        uint64_t seq = 0;
        {
            // `each` is the setting that makes this a promise rather than a hope
            barch::mq::queue q(path, {barch::sync_when::each_add, 0});
            seq = q.publish("survive me");
            // nothing else: no remove, no clean shutdown
        }
        barch::mq::queue q(path, {barch::sync_when::each_add, 0});
        barch::mq::message m;
        check(q.size() == 1, "it is still there after the queue was dropped");
        check(q.peek(m) && m.data == "survive me" && m.sequence == seq,
              "with its bytes and its sequence");
        const auto next = q.publish("after");
        check(next == seq + 1, "and the sequence carries on from what was in the file");
    }

    std::printf("a handler that fails leaves the message alone\n");
    {
        fresh();
        barch::mq::queue q(path, {barch::sync_when::never, 0}, 3);
        q.publish("keeps failing");
        q.publish("behind it");
        barch::mq::message m;

        check(q.peek(m) && m.data == "keeps failing", "the first is handed over");
        check(!q.failed(m), "a first failure does not dead letter it");
        check(q.size() == 2, "and nothing was dropped");
        check(q.peek(m) && m.data == "keeps failing" && m.attempts == 1,
              "the same message comes back, with its attempt counted");
        check(!q.failed(m), "a second failure, still not dead lettered");
        check(q.peek(m) && m.attempts == 2, "attempts keep counting");
        check(q.failed(m), "the third reaches max_attempts and dead letters it");
        check(q.size() == 1, "it left the queue");
        check(q.dead_size() == 1, "and is in the dead letter queue");
        barch::mq::message d;
        check(q.peek_dead(d) && d.data == "keeps failing" && d.sequence == m.sequence,
              "with the same bytes and the same sequence");
        check(q.peek(m) && m.data == "behind it",
              "and the message behind it is now the head - the queue is not stuck");
        check(m.attempts == 0, "which has no attempts of its own");
    }

    std::printf("a success clears the attempts it had\n");
    {
        fresh();
        barch::mq::queue q(path, {barch::sync_when::never, 0}, 3);
        q.publish("flaky");
        barch::mq::message m;
        check(q.peek(m), "handed over");
        check(!q.failed(m), "fails once");
        check(q.peek(m) && m.attempts == 1, "one attempt against it");
        q.remove();                                  // this time the handler worked
        const auto again = q.publish("flaky");
        check(q.peek(m) && m.sequence == again && m.attempts == 0,
              "a later message does not inherit the dead one's count");
    }

    std::printf("a record that does not verify is dropped, not handed over\n");
    {
        fresh();
        uint64_t good = 0;
        {
            barch::mq::queue q(path, {barch::sync_when::never, 0});
            q.publish("torn");
            good = q.publish("intact");
        }
        // flip a bit in the first message's payload, where a torn write lands.
        // 32 is the queue file header, 4 the element length, 16 the record header
        const int fd = ::open(path.c_str(), O_RDWR);
        if (fd >= 0) {
            uint8_t b = 0;
            if (::pread(fd, &b, 1, 32 + 4 + 16) == 1) {
                b ^= 0xFF;
                if (::pwrite(fd, &b, 1, 32 + 4 + 16) != 1) std::perror("pwrite");
            }
            ::close(fd);
        }
        barch::mq::queue q(path, {barch::sync_when::never, 0});
        barch::mq::message m;
        check(q.peek(m) && m.data == "intact" && m.sequence == good,
              "the good message behind the torn one is what comes back");
        check(q.size() == 1, "and the torn one is gone rather than stuck at the head");
    }

    ::unlink(path.c_str());
    ::unlink(dead.c_str());
    std::printf("\n%s\n", failures == 0 ? "all message queue checks pass"
                                       : "FAILURES: see above");
    return failures == 0 ? 0 : 1;
}
