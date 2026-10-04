// Windows stand-in for <arpa/inet.h>: the socket api is winsock's. See win32/README.md.
#ifndef BARCH_WIN32_ARPA_INET_H
#define BARCH_WIN32_ARPA_INET_H
#include <winsock2.h>
#include <ws2tcpip.h>
#endif
