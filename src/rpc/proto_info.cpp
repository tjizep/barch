//
// Created by teejip on 1/30/26.
//

#include "proto_info.h"

std::string remote_address_off(const tcp::socket& sock) {
    auto rep = sock.lowest_layer().remote_endpoint();
    return rep.address().to_string() +":"+ std::to_string(rep.port());
}

std::string local_address_off(const tcp::socket& sock) {
    auto rep = sock.lowest_layer().local_endpoint();
    return rep.address().to_string() + +":"+ std::to_string(rep.port());
}

std::string remote_address_off(const asio::basic_stream_socket<asio::local::stream_protocol>& sock) {
    auto rep = sock.lowest_layer().remote_endpoint();
    return rep.path();
}
std::string address_off(const asio::local::stream_protocol::endpoint& ep) {
    return ep.path();
}
std::string address_off(const tcp::endpoint& ep) {
    return ep.address().to_string();
}

std::string local_address_off(const asio::basic_stream_socket<asio::local::stream_protocol>& sock) {
    auto rep = sock.lowest_layer().remote_endpoint();
    return rep.path();
}
std::string proto_name(const asio::local::stream_protocol::endpoint& ) {
    return "UDS";
}
std::string proto_name(const tcp::endpoint& ) {
    return "TCP";
}