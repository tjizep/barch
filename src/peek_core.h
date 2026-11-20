#ifndef BARCH_PEEK_CORE_H
#define BARCH_PEEK_CORE_H

/*
 * What a peek of a socket answers - TODO 934. The collector's backup rule frees a
 * session nothing holds once its peek reads 0. Linux reads 0 after a FIN, after a
 * reset once its error was taken, and after the session shut its own socket.
 * Windows reads 0 after a FIN only. A reset fails every peek with WSAECONNRESET,
 * and a socket shut on both sides fails with WSAESHUTDOWN. So on Windows a reset or
 * shut session never peeked as 0. socket_peek.h sorts recv's error into one of the
 * kinds below, and this says what the peek answers.
 */
namespace barch::peek {
    enum class fail {
        would_block,    // nothing there yet: the socket is open
        gone,           // reset, aborted, shut or no longer connected
        other           // anything else: not taken as closed
    };

    /** recv's count when it is 0 or more, else what its error means: 0 for gone */
    constexpr long answer(long n, fail f) {
        if (n >= 0)
            return n;
        return f == fail::gone ? 0 : -1;
    }
}
#endif //BARCH_PEEK_CORE_H
