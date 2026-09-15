#pragma once
//
// Recording what arrives, so it can be run again - see TODO 316, 317 and 318.
//
// Off unless `traffic_capture` is on, and when it is off this costs one relaxed
// atomic load per command. When it is on, every command a RESP client sends is
// appended to a file - one file per thread that records, named from
// `traffic_file`.
//
// ONE FILE PER THREAD, because a single file is a single lock. Every session
// thread writing to one FILE* means eight threads queueing on its internal lock
// at two thirds of a million commands a second, which is a synchronisation point
// nobody asked for. A thread that owns its file contends with nobody: the lock is
// still taken, and it is always free.
//
// A FILE AND NOT A KEY SPACE. The first version wrote one key per command, which
// meant a recording doubled the write rate, competed with the data for arena
// pages, was saved with everything else, was eventually compressed by the
// background pass, and cost 820 MB of server memory for five million records. See
// DONE 306 for the measurements that argued against it.
//
// THE FILES. `traffic_file` names the set, and the thread index goes in front of
// the extension, so `barch_traffic.dat` is written as
//
//     barch_traffic.0.dat  barch_traffic.1.dat  ...
//
// Each one starts with
//
//     barch-traffic-file-1\n
//
// and then holds one record after another:
//
//     <bytes>\n<nanos>\n<connection>\n<space>\n<argc>\n(<len>\n<bytes>\n)*
//
// `<bytes>` is the length of everything after that first newline, so a reader can
// take a record without understanding it, and a reader that meets a short one has
// found the end of a file somebody was still writing - which is the normal way a
// recording ends, since nothing closes it if the process dies. `<nanos>` is the
// arrival time on the wall clock and `<connection>` the client id it came in on:
// without the second one a replay is a single stream, which reproduces the
// commands but never the concurrency. `<space>` is the space the command actually
// ran against, empty for the default one, so a replay can put the `space:CMD`
// prefix back. The arguments are length prefixed rather than delimited, because
// an argument can hold any byte including the newline - only the lengths are text.
//
// A reader puts the files back in order by `<nanos>`, which is why it is in the
// record rather than implied by position. Ordering across threads is now the
// clock's job rather than a lock's; within one connection it does not matter,
// because a connection's commands are sequential in real time whichever file they
// land in.
//
// HTTP REQUESTS go in the same file, as the arguments of a pseudo command called
// `HTTP` - see TODO 319:
//
//     HTTP  <method>  <raw url>  <port>  <content type>  <body>
//     [ <header name>  <header value> ]...
//
// The header pairs are whatever `traffic_headers` names, and that is empty by
// default so there are none - see TODO 321. They go after the body rather than
// into a block of their own so that the format did not have to change: the
// record already carries its own argument count, and a reader that does not
// know about headers still gets the request. The one worth naming is `Cookie`,
// without which a replayed session is signed out; with it a recording holds a
// live session, which is the reason it is opt in.
//
// One format, one reader and one timeline, so a recording of a web application
// and of the commands underneath it is one recording that replays together. The
// `<port>` is the HTTP port it arrived on, so a replay knows where to send it
// without being told. Crow's request carries no connection id - only the remote
// address - so the connection field is a hash of that address and a replay cannot
// put an HTTP request back on the socket it came in on; it fires them from a pool
// at their own offsets instead.
//
// WHAT IS NOT RECORDED, deliberately:
//
//  - CONFIG, in any form. The name is taken apart before it is checked, because
//    the dispatcher leaves the `<space>:` prefix on `args[0]`: `shop:CONFIG` is
//    the same command and was getting through. Replaying a `CONFIG SET` would
//    reconfigure the server underneath the replay, starting with turning capture
//    back on.
//  - the credential arguments of AUTH, HELLO ... AUTH and ACL SETUSER. They are
//    written as `<redacted>`, so the command and its shape are in the recording
//    and the password is not. The recorder runs before authorization and sees
//    every attempt, accepted or not; a recording is meant to be copied around,
//    and a file full of plaintext passwords is not something to hand anybody.
//    See TODO 328.
//
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace barch::traffic {
    /** is capture on right now. cheap enough to ask per command */
    bool capturing();
    /**
     * write one command. `conn` is the client id it arrived on and `space` the
     * canonical space name, empty for the default. Never throws: a recorder that
     * can break the command it is recording is worse than no recorder, so a
     * failed write is counted and dropped.
     */
    void record(uint64_t conn, std::string_view space,
                const std::vector<std::string_view>& args);
    /**
     * capture was turned on or off, or pointed at another file set.
     *
     * Every thread's file is flushed, which is what makes the last records of a
     * recording readable - turn capture off and the files are complete. Flushed
     * and not closed: a file belongs to the thread writing it, and closing one
     * from here would be closing it under a writer. Each owner closes its own at
     * its next record, or when the thread ends.
     */
    void capture_changed();
    /** how many commands have been written and how many dropped, since startup */
    uint64_t recorded();
    uint64_t dropped();
    /**
     * how many bytes this process has appended, across every thread's file.
     * Counted in batches rather than per record, so it lags by a little under
     * load - `traffic_max_bytes` is compared against the part of it written
     * since capture was last turned on, and is a bound on a recording rather
     * than an exact size.
     */
    uint64_t bytes_written();
    /** how many files this recording is spread over */
    uint64_t files();
}
