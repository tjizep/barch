//
// Created by teejip on 8/12/25.
//

#ifndef BARCH_URING_CONTEXT_H
#define BARCH_URING_CONTEXT_H
#include "ioutil.h"
#include "value_type.h"
#include <liburing.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Weffc++"
//#define ASIO_HAS_IO_URING
//#define ASIO_DISABLE_EPOLL
#include <asio/io_context.hpp>
#include <asio/detached.hpp>
#include <asio/ip/tcp.hpp>
#include <asio.hpp>
#pragma GCC diagnostic pop
enum {
    u_read = 1,
    u_write = 2,
    u_cancel = 3,
    u_shutdown = 4
};
enum {
    opt_uring_queue_size = 8192,
    opt_debug_uring = 0
};
using asio::ip::tcp;
using asio::local::stream_protocol;

typedef std::function<void(art::value_type data)> uring_cb;
struct request {
    request(){}
    ~request() {
    }
    uring_cb f{};
    art::value_type data{};
    int type{};
    int client_socket{};
};

struct uring_context {
    io_uring ring{};
    size_t id{};
    size_t operations_pending{};
    bool running = false;
    io_uring_cqe *cqe{};
    uring_context() {

    }
    ~uring_context() {
        stop();
    }
    uring_context(const uring_context&) = delete;
    uring_context& operator=(const uring_context&) = delete;
    bool start(size_t id) {
        if (running) return false;
        io_uring_queue_init(opt_uring_queue_size, &ring, 0);
        this->id = id;
        running = true;
        for (;run();) {
            --operations_pending;
        };
        running = false;
        return true;
    }
    int fd_is_valid(int fd)
    {
        return fcntl(fd, F_GETFD) != -1 || errno != EBADF;
    }
    void stop() {
        if (!running) return;
        request r;
        r.type = u_shutdown;
        write(0,r,art::value_type(),nullptr);
        while (running) {

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        io_uring_queue_exit(&ring);


    }
    bool run() {

        int ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret < 0) {
            art::std_err("io_uring_wait_cqe failed with",ret,", this queue will expire, id:",id);
            return false;
        }
        auto *req = (request *) cqe->user_data;
        if(req == nullptr) {
            art::std_err("invalid user data",cqe->res);
            io_uring_cqe_seen(&ring, cqe);
            return true;
        }
        if (opt_debug_uring == 1)
            art::std_log("req",req->type,req->client_socket,cqe->res);
        if (req->client_socket == 0) {
            io_uring_cqe_seen(&ring, cqe);
            return false;
        }
        if (cqe->res == 0 ) {
            io_uring_cqe_seen(&ring, cqe);
            if (opt_debug_uring == 1)
                art::std_log("exit detected",req->type,req->client_socket,cqe->res);
            auto f = std::move(req->f);

            return true;
        }
        if (req->type == u_read || req->type == u_write) {
            try {
                auto f = std::move(req->f);
                req->f = nullptr;
                art::value_type v = req->data.sub(0,cqe->res);
                if (opt_debug_uring == 1) {
                    std::string s = std::regex_replace(v.to_string(),std::regex("[\r\n]"),".");
                    art::std_log(req->type == u_read ? "read" : "write",s);
                }
                if (f)
                    f(v);
                else {
                    art::std_err("no callback provided on",(void*)req);
                }

            }catch (std::exception& e) {
                art::std_err("exception in callback",e.what());
            }
            net_stat stat;
            if (req->type == u_read ) {
                stream_read_ctr += cqe->res;
            }else {
                stream_write_ctr += cqe->res;
            }
        }else {
            art::std_err("invalid type",req->type);
        }
        io_uring_cqe_seen(&ring, cqe);

        return true;
    }
    void close(tcp::socket &s, request& req,uring_cb f) {
        if (opt_debug_uring==1)
            art::std_log("close",s.native_handle());
        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        req.client_socket = s.native_handle();
        req.type = u_shutdown;
        req.f = std::move(f);

        io_uring_prep_shutdown(sqe, req.client_socket, 0);
        io_uring_sqe_set_data(sqe, &req);
        int r = io_uring_submit(&ring);
        if (r < 0) {
            art::std_err("io_uring_submit failed (read)",r);
            req.f = nullptr;
        }else
            ++operations_pending;
    }
    void cancel(tcp::socket &s, request& req,uring_cb f) {
        if (opt_debug_uring==1)
            art::std_log("cancel",s.native_handle());
        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        req.client_socket = s.native_handle();
        req.type = u_cancel;
        req.f = std::move(f);

        io_uring_prep_cancel_fd(sqe, req.client_socket, 0);
        io_uring_sqe_set_data(sqe, &req);
        int r = io_uring_submit(&ring);
        if (r < 0) {
            art::std_err("io_uring_submit failed (read)",r);
            req.f = nullptr;
        }else {
            ++operations_pending;
        }
    }
    void read(tcp::socket &s, request& req, art::value_type value, uring_cb f) {
        if (opt_debug_uring==1)
            art::std_log("read",s.native_handle(),value.size);
        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        req.client_socket = s.native_handle();
        req.type = u_read;
        req.f = std::move(f);
        req.data = value;
        /* Linux kernel 5.5 has support for readv, but not for recv() or read() */

        io_uring_prep_read(sqe, req.client_socket, (void*)value.bytes, value.size, 0);
        io_uring_sqe_set_data(sqe, &req);
        int r = io_uring_submit(&ring);
        if (r < 0) {
            art::std_err("io_uring_submit failed (read)",r);
            req.f = nullptr;
        }else {
            ++operations_pending;
        }
    }
    void write(tcp::socket::native_handle_type s, request& req, art::value_type value, uring_cb f) {
        if (opt_debug_uring==1)
            art::std_log("write",s,value.size);
        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        req.type = u_write;
        req.f = std::move(f);
        req.client_socket = s;
        req.data = value;
        io_uring_prep_write(sqe, s, (void *)value.bytes, value.size, 0);
        io_uring_sqe_set_data(sqe, &req);
        int r = io_uring_submit(&ring);
        if (r < 0) {
            art::std_err("io_uring_submit failed (write)",r);
            req.f = nullptr;
        }else {
            ++operations_pending;
        }
    }

    void write(tcp::socket &s, request& req, art::value_type value, uring_cb f) {
        write(s.native_handle(),req,value,std::move(f));
    }
};
#endif //BARCH_URING_CONTEXT_H