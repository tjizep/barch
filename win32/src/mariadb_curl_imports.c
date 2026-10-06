/*
 * MSYS2's libmariadbclient.a was built to call curl from libcurl.dll (its
 * remote_io plugin), so its objects reference curl through __imp_ pointers -
 * the indirection a dll import goes through. barch links curl statically, which
 * has the functions but no such pointers. These are those pointers, aimed at the
 * static curl, so the client links into one exe with everything else - TODO 601.
 *
 * Only the functions the archive actually asks for; a new one shows up as an
 * undefined __imp_curl_* at link time and goes on the list.
 */
#ifndef CURL_STATICLIB
#define CURL_STATICLIB
#endif
#include <curl/curl.h>

#define BARCH_CURL_IMPORT(fn) void *__imp_##fn = (void *) fn;

BARCH_CURL_IMPORT(curl_global_init)
BARCH_CURL_IMPORT(curl_global_cleanup)
BARCH_CURL_IMPORT(curl_easy_init)
BARCH_CURL_IMPORT(curl_easy_setopt)
BARCH_CURL_IMPORT(curl_easy_cleanup)
BARCH_CURL_IMPORT(curl_multi_init)
BARCH_CURL_IMPORT(curl_multi_add_handle)
BARCH_CURL_IMPORT(curl_multi_remove_handle)
BARCH_CURL_IMPORT(curl_multi_perform)
BARCH_CURL_IMPORT(curl_multi_timeout)
BARCH_CURL_IMPORT(curl_multi_fdset)
BARCH_CURL_IMPORT(curl_multi_cleanup)
