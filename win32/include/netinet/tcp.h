// Windows stand-in for <netinet/tcp.h>: the socket api is winsock's. See win32/README.md.
#ifndef BARCH_WIN32_NETINET_TCP_H
#define BARCH_WIN32_NETINET_TCP_H
#include <winsock2.h>
#include <ws2tcpip.h>
#endif
