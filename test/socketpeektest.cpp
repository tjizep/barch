/*
 * What barch::peek_now reads on a real socket - TODO 934.
 *
 * The session collector's backup rule frees a session nothing holds once a peek of
 * its socket reads 0. That needs 0 after the peer's FIN or reset, and after the
 * session shut its own socket. Linux and Windows peek in different ways, so each
 * case runs on a loopback pair. A wait is a select with no timeout on the socket,
 * never a clock. Built on Linux by CMake and on Windows by win32/CMakeLists.txt.
 *
 * On Windows, before this, a reset peeked -1 on every call. Linux peeked -1 then 0.
 * Now both read 0 at once. A socket the session shut itself still peeks -1 on
 * Windows, since select does not mark it readable. That case sets let_go anyway.
 * The rule is peek::answer in peek_core.h, checked first over every input it takes.
 */
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
typedef SOCKET sock_t;
#define SHUT_WR SD_SEND
#define SHUT_RDWR SD_BOTH
static void close_sock(sock_t s) { closesocket(s); }
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
typedef int sock_t;
static void close_sock(sock_t s) { close(s); }
#endif
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include "socket_peek.h"
#include "peek_core.h"

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-70s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

/** a connected loopback pair: [0] is the server's end, [1] the client's */
static bool pair(sock_t out[2]) {
    sock_t l = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;
    socklen_t n = sizeof a;
    if (bind(l, (sockaddr*) &a, sizeof a) != 0 || listen(l, 1) != 0
        || getsockname(l, (sockaddr*) &a, &n) != 0)
        return false;
    out[1] = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(out[1], (sockaddr*) &a, sizeof a) != 0)
        return false;
    out[0] = accept(l, nullptr, nullptr);
    close_sock(l);
    return true;
}

/** wait, with no timeout, until `s` has something to read: bytes, a FIN or a reset */
static void wait_readable(sock_t s) {
    fd_set r;
    FD_ZERO(&r);
    FD_SET(s, &r);
    select((int) s + 1, &r, nullptr, nullptr, nullptr);
}

static void reset(sock_t s) {
    linger l{};
    l.l_onoff = 1;
    l.l_linger = 0;
    setsockopt(s, SOL_SOCKET, SO_LINGER, (const char*) &l, sizeof l);
    close_sock(s);
}

using peek_rule = long (*)(long, barch::peek::fail);

/** the first input a rule answers wrongly, or nullptr */
static const char* rule_fault(peek_rule rule) {
    using barch::peek::fail;
    for (long n : {-1L, 0L, 1L}) {
        for (fail f : {fail::would_block, fail::gone, fail::other}) {
            const long a = rule(n, f);
            if (n > 0 && a != n) return "bytes waiting do not peek as bytes";
            if (n == 0 && a != 0) return "a FIN does not peek as 0";
            if (n < 0 && f == fail::gone && a != 0) return "a gone socket does not peek as 0";
            if (n < 0 && f != fail::gone && a != -1) return "an open socket peeks as closed";
        }
    }
    return nullptr;
}

static void rule_caught(const char* what, peek_rule rule, const char* why) {
    const char* f = rule_fault(rule);
    check(f && std::strcmp(f, why) == 0, what);
    std::printf("  %s\n", f ? f : "(nothing found)");
}

int main() {
    std::printf("the peek rule, every input - TODO 934\n");
    check(rule_fault(barch::peek::answer) == nullptr, "as now");
    rule_caught("catches the rule before TODO 934, a gone socket as -1",
                [](long n, barch::peek::fail) { return n >= 0 ? n : -1L; },
                "a gone socket does not peek as 0");
    rule_caught("catches every error as gone",
                [](long n, barch::peek::fail) { return n >= 0 ? n : 0L; },
                "an open socket peeks as closed");
    rule_caught("catches a byte count read as a flag",
                [](long n, barch::peek::fail f) { return n > 0 ? 2L : barch::peek::answer(n, f); },
                "bytes waiting do not peek as bytes");

#ifdef _WIN32
    WSADATA w;
    WSAStartup(MAKEWORD(2, 2), &w);
#endif
    sock_t p[2];
    char b[4];

    std::printf("an open connection\n");
    if (!pair(p)) { std::printf("no loopback pair\n"); return 1; }
    long n = barch::peek_now(p[0]);
    check(n == -1 && errno == EAGAIN, "nothing sent: -1 and EAGAIN, and no wait");
    send(p[1], "x", 1, 0);
    wait_readable(p[0]);
    check(barch::peek_now(p[0]) == 1, "a byte sent: 1");
    recv(p[0], b, 1, 0);
    check(barch::peek_now(p[0]) == -1 && errno == EAGAIN, "the byte read: -1 and EAGAIN again");

    std::printf("a FIN\n");
    send(p[1], "x", 1, 0);
    shutdown(p[1], SHUT_WR);
    wait_readable(p[0]);
    check(barch::peek_now(p[0]) == 1, "a byte before the FIN: 1, not 0");
    recv(p[0], b, 1, 0);
    check(barch::peek_now(p[0]) == 0, "the byte read: 0");
    check(barch::peek_now(p[0]) == 0, "and 0 again");
    close_sock(p[0]);
    close_sock(p[1]);

    std::printf("a reset\n");
    if (!pair(p)) { std::printf("no loopback pair\n"); return 1; }
    reset(p[1]);
    wait_readable(p[0]);
    const long first = barch::peek_now(p[0]);
    const int first_errno = errno;
    const long second = barch::peek_now(p[0]);
    std::printf("  peeks %ld (errno %d), then %ld\n", first, first ? first_errno : 0, second);
    check(first == 0, "the first peek: 0");
    check(second == 0, "the second peek: 0");
    close_sock(p[0]);

    std::printf("the session shuts its own socket\n");
    if (!pair(p)) { std::printf("no loopback pair\n"); return 1; }
    shutdown(p[0], SHUT_RDWR);
    const long shut = barch::peek_now(p[0]);
    std::printf("  peeks %ld (errno %d)\n", shut, shut ? errno : 0);
#ifdef _WIN32
    // select does not mark a socket readable after its own shutdown (wine), so the
    // peek says EAGAIN. Every path that shuts a session sets let_go as well, so the
    // collector does not need this peek
    check(shut == 0 || (shut == -1 && errno == EAGAIN), "after shutdown both: 0, or EAGAIN on Windows");
#else
    check(shut == 0, "after shutdown both: 0");
#endif
    close_sock(p[0]);
    close_sock(p[1]);

    std::printf("%s\n", failures ? "FAILED" : "all passed");
    return failures ? 1 : 0;
}
