# Keep a connection alive until its response's end() returns.
#
# A handler that leaves its response open gets a complete_request_handler_ that
# holds the connection. end() calls it, and the connection's complete_request()
# clears that same handler while it runs. When nothing else holds the connection,
# as with a `Connection: close` request answered later, clearing it destroys the
# connection, its response and the handler inside the handler's own call: a
# double free. A handler parked on I/O ends its response that way. See TODO 911.
#
# The fix calls a copy, so the connection lives until end() is done with its own
# members. Same shape as crow_tcp_nodelay.cmake: idempotent, and a configure that
# can't find the anchor fails rather than building the double free back in.

if (NOT DEFINED CROW_HTTP_RESPONSE_H)
    message(FATAL_ERROR "crow_deferred_end: CROW_HTTP_RESPONSE_H is not set")
endif ()
if (NOT EXISTS "${CROW_HTTP_RESPONSE_H}")
    message(FATAL_ERROR "crow_deferred_end: no such file ${CROW_HTTP_RESPONSE_H}")
endif ()

file(READ "${CROW_HTTP_RESPONSE_H}" _crow_src)

if (_crow_src MATCHES "barch_deferred_end")
    return()                                    # already patched
endif ()

set(_anchor "                if (complete_request_handler_)
                {
                    complete_request_handler_();
                    manual_length_header = false;
                    skip_body = false;
                }")
set(_patched "                if (complete_request_handler_)
                {
                    // barch_deferred_end: the handler clears itself, and it may
                    // hold the last reference to this response's connection. The
                    // copy keeps the connection until this returns. See TODO 911.
                    auto barch_deferred_end = complete_request_handler_;
                    barch_deferred_end();
                    manual_length_header = false;
                    skip_body = false;
                }")

string(FIND "${_crow_src}" "${_anchor}" _at)
if (_at EQUAL -1)
    message(FATAL_ERROR
        "crow_deferred_end: response::end() in ${CROW_HTTP_RESPONSE_H} is not the shape "
        "this patch expects. Crow has changed - check whether end() still runs a "
        "handler that clears itself, and if so, update the anchor. See TODO 911.")
endif ()

string(REPLACE "${_anchor}" "${_patched}" _crow_src "${_crow_src}")
file(WRITE "${CROW_HTTP_RESPONSE_H}" "${_crow_src}")
message(STATUS "Patched Crow to keep a connection through end() (TODO 911)")
