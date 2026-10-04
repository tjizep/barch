#ifndef BARCH_SOCKET_PEEK_H
#define BARCH_SOCKET_PEEK_H

#include <cerrno>
#ifndef _WIN32
#include <sys/socket.h>
#endif

namespace barch {
    /**
     * Look at the next byte on a socket without taking it and without waiting -
     * recv(MSG_PEEK | MSG_DONTWAIT) on Linux. 1 when bytes are waiting, 0 when the
     * peer has closed, and -1 otherwise with errno set: EAGAIN when there's simply
     * nothing there yet.
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
        const int n = r < 0 ? -1 : ::recv(fd, &b, 1, MSG_PEEK);
        if (n < 0)
            errno = ECONNRESET;
        return n;
#else
        return (long) ::recv(fd, &b, 1, MSG_PEEK | MSG_DONTWAIT);
#endif
    }
}

#endif
