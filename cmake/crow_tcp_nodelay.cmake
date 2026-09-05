# Set TCP_NODELAY on a connection Crow accepts.
#
# Crow never sets it, and its response leaves as two sendmsg calls - asio caps a
# scatter-gather write at 16 iovecs and Crow builds a buffer per header token, so the
# headers go in one segment and the body in the next. That is write-write-read with
# Nagle enabled: the second small write waits for an ACK the client delays, and every
# keep-alive request after the first on a connection costs 40ms. Measured, and gone
# with this: 41ms to 0.04ms. See DONE 225 and TODO 234.
#
# Applied as a script rather than a diff so it is idempotent - a build directory whose
# crow-src is already populated gets patched on the next configure, and one that is
# already patched is left alone. If the anchor is not found the configure fails rather
# than quietly producing a server with the 40ms back in it, which is the whole reason
# this is not a `sed` in a PATCH_COMMAND.

if (NOT DEFINED CROW_HTTP_SERVER_H)
    message(FATAL_ERROR "crow_tcp_nodelay: CROW_HTTP_SERVER_H is not set")
endif ()
if (NOT EXISTS "${CROW_HTTP_SERVER_H}")
    message(FATAL_ERROR "crow_tcp_nodelay: no such file ${CROW_HTTP_SERVER_H}")
endif ()

file(READ "${CROW_HTTP_SERVER_H}" _crow_src)

if (_crow_src MATCHES "barch_tcp_nodelay")
    return()                                    # already patched
endif ()

set(_anchor "                  [this, p, &ic, context_idx](error_code ec) {
                      if (!ec)
                      {
                          asio::post(ic,")
set(_patched "                  [this, p, &ic, context_idx](error_code ec) {
                      if (!ec)
                      {
                          // barch_tcp_nodelay: without this every keep-alive request
                          // after the first on a connection waits 40ms for a delayed
                          // ACK, because a response leaves as two writes. See DONE 225.
                          error_code barch_nodelay_ec;
                          p->socket().set_option(asio::ip::tcp::no_delay(true), barch_nodelay_ec);
                          asio::post(ic,")

string(FIND "${_crow_src}" "${_anchor}" _at)
if (_at EQUAL -1)
    message(FATAL_ERROR
        "crow_tcp_nodelay: the accept handler in ${CROW_HTTP_SERVER_H} is not the shape "
        "this patch expects. Crow has changed - check whether it sets TCP_NODELAY "
        "itself now, and if not, update the anchor. See TODO 234.")
endif ()

string(REPLACE "${_anchor}" "${_patched}" _crow_src "${_crow_src}")
file(WRITE "${CROW_HTTP_SERVER_H}" "${_crow_src}")
message(STATUS "Patched Crow for TCP_NODELAY on accept (DONE 225)")
