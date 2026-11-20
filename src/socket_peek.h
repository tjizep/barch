#ifndef BARCH_SOCKET_PEEK_H
#define BARCH_SOCKET_PEEK_H

#include <cerrno>
#ifndef _WIN32
#include <sys/socket.h>
#endif
#include "peek_core.h"

namespace barch {
    /**
     * Look at the next byte on a socket without taking it and without waiting -
     * recv(MSG_PEEK | MSG_DONTWAIT) on Linux. 1 when bytes are waiting, 0 when the
     * peer has closed or reset, or the socket is shut, and -1 otherwise with errno
     * set: EAGAIN when there's simply nothing there yet. See peek_core.h, TODO 934.
     *
     * Windows has no MSG_DONTWAIT, and asio leaves its sockets blocking there, so a
     * plain peek would wait for the next byte. A select with no timeout says first
     * whether it would.
     */
    template <typename Fd>
    long peek_now(Fd fd) {
        char b;
#ifdef _WIN32
        fd_set readable;
        FD_ZERO(&readable);
        FD_SET(fd, &readable);
        timeval now{0, 0};
        const int r = ::select(0, &readable, nullptr, nullptr, &now);
        if (r == 0) {
            errno = EAGAIN;
            return -1;
        }
        const long n = r < 0 ? -1 : ::recv(fd, &b, 1, MSG_PEEK);
        if (n >= 0)
            return n;
        const int e = r < 0 ? 0 : WSAGetLastError();
        const auto f = e == WSAEWOULDBLOCK ? peek::fail::would_block
                     : e == WSAECONNRESET || e == WSAECONNABORTED || e == WSAESHUTDOWN
                       || e == WSAENOTCONN || e == WSAENETRESET ? peek::fail::gone
                     : peek::fail::other;
        errno = f == peek::fail::would_block ? EAGAIN : ECONNRESET;
#else
        const long n = (long) ::recv(fd, &b, 1, MSG_PEEK | MSG_DONTWAIT);
        if (n >= 0)
            return n;
        const int e = errno;
        const auto f = e == EAGAIN || e == EWOULDBLOCK ? peek::fail::would_block
                     : e == ECONNRESET || e == ECONNABORTED || e == ENOTCONN || e == EPIPE
                       ? peek::fail::gone
                     : peek::fail::other;
#endif
        return peek::answer(n, f);
    }
}

#endif
