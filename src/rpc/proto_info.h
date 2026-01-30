//
// Created by teejip on 1/30/26.
//

#ifndef BARCH_PROTO_INFO_H
#define BARCH_PROTO_INFO_H

#include "asio_includes.h"
std::string remote_address_off(const tcp::socket& sock);
std::string local_address_off(const tcp::socket& sock);
std::string remote_address_off(const asio::basic_stream_socket<asio::local::stream_protocol>& sock);
std::string address_off(const asio::local::stream_protocol::endpoint& ep) ;
std::string address_off(const tcp::endpoint& ep) ;
std::string proto_name(const asio::local::stream_protocol::endpoint& ep) ;
std::string proto_name(const tcp::endpoint& ep) ;
std::string local_address_off(const asio::basic_stream_socket<asio::local::stream_protocol>& sock);


#endif //BARCH_PROTO_INFO_H