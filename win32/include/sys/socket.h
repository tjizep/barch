// Windows stand-in for <sys/socket.h>: the socket api is winsock's. See win32/README.md.
#ifndef BARCH_WIN32_SYS_SOCKET_H
#define BARCH_WIN32_SYS_SOCKET_H
#include <winsock2.h>
#include <ws2tcpip.h>
#endif
