//
// Created by teejip on 9/7/25.
//

#ifndef BARCH_ASIO_INCLUDES_H
#define BARCH_ASIO_INCLUDES_H
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Weffc++"
// IO_URING doesnt seem to work
//#define ASIO_HAS_IO_URING
//#define ASIO_DISABLE_EPOLL
#include <asio/io_context.hpp>
#include <asio/detached.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/write.hpp>
#include <asio.hpp>
#include "asio/io_service.hpp"
#pragma GCC diagnostic pop
using asio::ip::tcp;
using asio::local::stream_protocol;
#endif //BARCH_ASIO_INCLUDES_H