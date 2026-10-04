// Windows stand-in for <netinet/in.h>: the socket api is winsock's. See win32/README.md.
#ifndef BARCH_WIN32_NETINET_IN_H
#define BARCH_WIN32_NETINET_IN_H
#include <winsock2.h>
#include <ws2tcpip.h>
#endif
