
//
// Created by teejip on 8/12/25.
//

#ifndef BARCH_URING_CONTEXT_H
#define BARCH_URING_CONTEXT_H
#include "ioutil.h"
#include "value_type.h"
#include <liburing.h>
#include "asio_includes.h"
#include <bitset>
#include <sys/mman.h>

enum uring_op_type {
    u_read = 1,
    u_write = 2,
    u_cancel = 3,
    u_shutdown = 4
};

// Tunable parameters - adjust based on workload
struct uring_config {
    static constexpr size_t QUEUE_SIZE = 256;           // Was 16, increased for better batching
    static constexpr size_t SUBMIT_BATCH_SIZE = 0;      // Batch this many before submitting
    static constexpr size_t NUM_REGISTERED_BUFFERS = 64;
    static constexpr size_t REGISTERED_BUFFER_SIZE = 8192;
    static constexpr bool USE_SQPOLL = false;            // Kernel-side polling (reduces syscalls)
    static constexpr unsigned SQPOLL_IDLE_MS = 2000;    // Kernel thread sleep timeout
    static constexpr bool DEBUG = false;
};

typedef std::function<void(art::value_type data)> uring_cb;

struct request {
    request() = default;
    ~request() = default;

    uring_cb f{};
    art::value_type data{};
    int type{};
    int client_socket{};
    int16_t buffer_index{-1};  // Index into registered buffers, -1 if not using
};

class uring_context {
public:
    io_uring ring{};
    size_t id{};
    std::atomic<size_t> operations_pending{};
    std::atomic<size_t> pending_sqes{};  // Unsubmitted SQEs
    std::atomic<bool> running{false};
    io_uring_cqe *cqe{};

    // Registered buffer pool for zero-copy I/O
    std::vector<iovec> registered_buffers{};
    std::unique_ptr<char[]> buffer_pool{};
    std::bitset<uring_config::NUM_REGISTERED_BUFFERS> buffer_in_use{};
    std::mutex buffer_mutex{};

    bool buffers_registered{false};
    bool sqpoll_enabled{false};

    uring_context() = default;

    ~uring_context() {
        stop();
    }

    uring_context(const uring_context&) = delete;
    uring_context& operator=(const uring_context&) = delete;

    bool start(size_t tid) {
        if (running.exchange(true)) return false;

        // Try SQPOLL mode first for minimal syscall overhead
        io_uring_params params{};
        int ret = -1;

        if constexpr (uring_config::USE_SQPOLL) {
            params.flags = IORING_SETUP_SQPOLL;
            params.sq_thread_idle = uring_config::SQPOLL_IDLE_MS;

            // Pin SQPOLL thread to a specific CPU if possible
            params.flags |= IORING_SETUP_SQ_AFF;
            params.sq_thread_cpu = tid % std::thread::hardware_concurrency();

            ret = io_uring_queue_init_params(uring_config::QUEUE_SIZE, &ring, &params);
            if (ret >= 0) {
                sqpoll_enabled = true;
                if constexpr (uring_config::DEBUG)
                    barch::std_log("SQPOLL mode enabled for uring context", tid);
            }
        }

        // Fallback to regular mode
        if (ret < 0) {
            memset(&params, 0, sizeof(params));
            ret = io_uring_queue_init(uring_config::QUEUE_SIZE, &ring, 0);
            if (ret < 0) {
                barch::std_err("io_uring_queue_init failed:", ret);
                running = false;
                return false;
            }
        }

        // Setup registered buffers for zero-copy
        setup_registered_buffers();

        this->id = tid;

        // Main event loop
        while (run()) {
            // Process completions
        }

        cleanup();
        running = false;
        return true;
    }

    void setup_registered_buffers() {
        // Allocate page-aligned buffer pool
        size_t total_size = uring_config::NUM_REGISTERED_BUFFERS * uring_config::REGISTERED_BUFFER_SIZE;

        // Use mmap for page-aligned memory (better for DMA)
        void* ptr = mmap(nullptr, total_size,
                        PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
                        -1, 0);

        if (ptr == MAP_FAILED) {
            // Fallback to regular allocation
            buffer_pool = std::make_unique<char[]>(total_size);
            ptr = buffer_pool.get();
        }

        registered_buffers.resize(uring_config::NUM_REGISTERED_BUFFERS);

        for (size_t i = 0; i < uring_config::NUM_REGISTERED_BUFFERS; ++i) {
            registered_buffers[i].iov_base = static_cast<char*>(ptr) + (i * uring_config::REGISTERED_BUFFER_SIZE);
            registered_buffers[i].iov_len = uring_config::REGISTERED_BUFFER_SIZE;
        }

        int ret = io_uring_register_buffers(&ring, registered_buffers.data(),
                                            uring_config::NUM_REGISTERED_BUFFERS);
        if (ret < 0) {
            if constexpr (uring_config::DEBUG)
                barch::std_err("Failed to register buffers:", ret, "- using regular I/O");
            buffers_registered = false;
        } else {
            buffers_registered = true;
            if constexpr (uring_config::DEBUG)
                barch::std_log("Registered", uring_config::NUM_REGISTERED_BUFFERS, "buffers for zero-copy I/O");
        }
    }

    // Get a registered buffer index, returns -1 if none available
    int16_t acquire_buffer() {
        if (!buffers_registered) return -1;

        std::lock_guard lock(buffer_mutex);
        for (size_t i = 0; i < uring_config::NUM_REGISTERED_BUFFERS; ++i) {
            if (!buffer_in_use[i]) {
                buffer_in_use[i] = true;
                return static_cast<int16_t>(i);
            }
        }
        return -1;  // No buffer available
    }

    void release_buffer(int16_t index) {
        if (index < 0 || index >= static_cast<int16_t>(uring_config::NUM_REGISTERED_BUFFERS)) return;

        std::lock_guard lock(buffer_mutex);
        buffer_in_use[index] = false;
    }

    void* get_buffer_ptr(int16_t index) {
        if (index < 0) return nullptr;
        return registered_buffers[index].iov_base;
    }

    int fd_is_valid(int fd) {
        return fcntl(fd, F_GETFD) != -1 || errno != EBADF;
    }

    void stop() {
        if (!running) return;

        // Submit a shutdown request
        request r;
        r.type = u_shutdown;
        r.client_socket = 0;  // Signal shutdown

        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (sqe) {
            io_uring_prep_nop(sqe);
            io_uring_sqe_set_data(sqe, &r);
            io_uring_submit(&ring);
        }

        // Wait for shutdown
        int timeout_ms = 5000;
        while (running && timeout_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            timeout_ms -= 10;
        }
    }

    void cleanup() {
        // Unregister buffers
        if (buffers_registered) {
            io_uring_unregister_buffers(&ring);
            buffers_registered = false;
        }

        // Free mmap'd memory if used
        if (!buffer_pool && !registered_buffers.empty()) {
            size_t total_size = uring_config::NUM_REGISTERED_BUFFERS * uring_config::REGISTERED_BUFFER_SIZE;
            munmap(registered_buffers[0].iov_base, total_size);
        }

        io_uring_queue_exit(&ring);
    }

    // Flush pending submissions
    void flush_submissions() {
        size_t pending = pending_sqes.load();
        if (pending > 0) {
            int submitted = io_uring_submit(&ring);
            if (submitted > 0) {
                pending_sqes -= submitted;
                operations_pending += submitted;
            }
        }
    }

    // Maybe flush if we've accumulated enough
    void maybe_flush() {
        if (pending_sqes >= uring_config::SUBMIT_BATCH_SIZE) {
            flush_submissions();
        } else if (sqpoll_enabled) {
            // With SQPOLL, kernel will pick up submissions automatically
            // Just need to ensure the SQ is visible
            io_uring_sqring_wait(&ring);
        }
    }

    bool run() {
        // Flush any pending submissions before waiting
        flush_submissions();

        int ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret < 0) {
            if (ret == -EINTR) return true;  // Interrupted, continue
            barch::std_err("io_uring_wait_cqe failed:", ret, ", queue", id, "will exit");
            return false;
        }

        auto *req = static_cast<request*>(io_uring_cqe_get_data(cqe));
        if (req == nullptr) {
            if constexpr (uring_config::DEBUG)
                barch::std_err("null user_data, res:", cqe->res);
            io_uring_cqe_seen(&ring, cqe);
            return true;
        }

        if constexpr (uring_config::DEBUG)
            barch::std_log("cqe: type=", req->type, " fd=", req->client_socket, " res=", cqe->res);

        // Shutdown signal
        if (req->client_socket == 0) {
            io_uring_cqe_seen(&ring, cqe);
            return false;
        }

        // Connection closed or error
        if (cqe->res == 0) {
            io_uring_cqe_seen(&ring, cqe);
            if constexpr (uring_config::DEBUG)
                barch::std_log("connection closed: type=", req->type, " fd=", req->client_socket);

            // Release buffer if used
            if (req->buffer_index >= 0) {
                release_buffer(req->buffer_index);
                req->buffer_index = -1;
            }
            return true;
        }

        // Handle error
        if (cqe->res < 0) {
            if constexpr (uring_config::DEBUG)
                barch::std_err("I/O error:", cqe->res, " type:", req->type);
            io_uring_cqe_seen(&ring, cqe);

            if (req->buffer_index >= 0) {
                release_buffer(req->buffer_index);
                req->buffer_index = -1;
            }
            return true;
        }

        // Process completion
        if (req->type == u_read || req->type == u_write) {
            try {
                auto f = std::move(req->f);
                req->f = nullptr;

                art::value_type v = req->data.sub(0, cqe->res);

                if constexpr (uring_config::DEBUG) {
                    std::string s = std::regex_replace(v.to_string(), std::regex("[\r\n]"), ".");
                    barch::std_log(req->type == u_read ? "read" : "write", s.substr(0, 64));
                }

                if (f) {
                    f(v);
                }

            } catch (std::exception& e) {
                barch::std_err("callback exception:", e.what());
            }

            // Update statistics
            net_stat stat;
            if (req->type == u_read) {
                stream_read_ctr += cqe->res;
            } else {
                stream_write_ctr += cqe->res;
            }

            // Release registered buffer
            if (req->buffer_index >= 0) {
                release_buffer(req->buffer_index);
                req->buffer_index = -1;
            }
        }

        io_uring_cqe_seen(&ring, cqe);
        --operations_pending;
        return true;
    }

    void close(tcp::socket &s, request& req, uring_cb f) {
        if constexpr (uring_config::DEBUG)
            barch::std_log("close fd:", s.native_handle());

        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (!sqe) {
            flush_submissions();
            sqe = io_uring_get_sqe(&ring);
            if (!sqe) {
                barch::std_err("SQ full, cannot submit close");
                return;
            }
        }

        req.client_socket = s.native_handle();
        req.type = u_shutdown;
        req.f = std::move(f);

        io_uring_prep_shutdown(sqe, req.client_socket, SHUT_RDWR);
        io_uring_sqe_set_data(sqe, &req);
        ++pending_sqes;
        maybe_flush();
    }

    void cancel(tcp::socket &s, request& req, uring_cb f) {
        if constexpr (uring_config::DEBUG)
            barch::std_log("cancel fd:", s.native_handle());

        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (!sqe) {
            flush_submissions();
            sqe = io_uring_get_sqe(&ring);
        }
        if (!sqe) return;

        req.client_socket = s.native_handle();
        req.type = u_cancel;
        req.f = std::move(f);

        io_uring_prep_cancel_fd(sqe, req.client_socket, 0);
        io_uring_sqe_set_data(sqe, &req);
        ++pending_sqes;
        maybe_flush();
    }

    // Read with optional registered buffer
    void read(tcp::socket &s, request& req, art::value_type value, uring_cb f) {
        if constexpr (uring_config::DEBUG)
            barch::std_log("read fd:", s.native_handle(), " size:", value.size);

        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (!sqe) {
            flush_submissions();
            sqe = io_uring_get_sqe(&ring);
            if (!sqe) {
                barch::std_err("SQ full, cannot submit read");
                return;
            }
        }

        req.client_socket = s.native_handle();
        req.type = u_read;
        req.f = std::move(f);
        req.data = value;

        // Try to use registered buffer for zero-copy
        int16_t buf_idx = acquire_buffer();
        if (buf_idx >= 0 && value.size <= uring_config::REGISTERED_BUFFER_SIZE) {
            req.buffer_index = buf_idx;
            req.data = art::value_type{
                static_cast<const char*>(get_buffer_ptr(buf_idx)),
                std::min(value.size, static_cast<unsigned>(uring_config::REGISTERED_BUFFER_SIZE))
            };

            io_uring_prep_read_fixed(sqe, req.client_socket,
                                     const_cast<char*>(req.data.chars()),
                                     req.data.size, 0, buf_idx);
        } else {
            req.buffer_index = -1;
            io_uring_prep_read(sqe, req.client_socket,
                              (char*)(value.bytes), value.size, 0);
        }

        io_uring_sqe_set_data(sqe, &req);
        ++pending_sqes;
        maybe_flush();
    }

    // Write with optional registered buffer
    void write(tcp::socket::native_handle_type s, request& req, art::value_type value, uring_cb f) {
        if constexpr (uring_config::DEBUG)
            barch::std_log("write fd:", s, " size:", value.size);

        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (!sqe) {
            flush_submissions();
            sqe = io_uring_get_sqe(&ring);
            if (!sqe) {
                barch::std_err("SQ full, cannot submit write");
                return;
            }
        }

        req.type = u_write;
        req.f = std::move(f);
        req.client_socket = s;
        req.data = value;

        // Try to use registered buffer
        int16_t buf_idx = acquire_buffer();
        if (buf_idx >= 0 && value.size <= uring_config::REGISTERED_BUFFER_SIZE) {
            req.buffer_index = buf_idx;
            void* buf_ptr = get_buffer_ptr(buf_idx);
            memcpy(buf_ptr, value.bytes, value.size);

            io_uring_prep_write_fixed(sqe, s, buf_ptr, value.size, 0, buf_idx);
        } else {
            req.buffer_index = -1;
            io_uring_prep_write(sqe, s, value.bytes, value.size, 0);
        }

        io_uring_sqe_set_data(sqe, &req);
        ++pending_sqes;
        maybe_flush();
    }

    void write(tcp::socket &s, request& req, art::value_type value, uring_cb f) {
        write(s.native_handle(), req, value, std::move(f));
    }

    // Batch write - submit multiple writes efficiently
    template<typename Container>
    void write_batch(tcp::socket &s, const Container& buffers, request& req, uring_cb f) {
        if (buffers.empty()) return;

        // For single buffer, use regular write
        if (buffers.size() == 1) {
            write(s, req, buffers[0], std::move(f));
            return;
        }

        // Use writev for multiple buffers
        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (!sqe) {
            flush_submissions();
            sqe = io_uring_get_sqe(&ring);
        }
        if (!sqe) return;

        req.type = u_write;
        req.f = std::move(f);
        req.client_socket = s.native_handle();
        req.buffer_index = -1;

        // Build iovec array
        thread_local std::vector<iovec> iovecs;
        iovecs.clear();
        iovecs.reserve(buffers.size());

        for (const auto& buf : buffers) {
            iovecs.push_back({const_cast<char*>(buf.bytes), buf.size});
        }

        io_uring_prep_writev(sqe, req.client_socket, iovecs.data(), iovecs.size(), 0);
        io_uring_sqe_set_data(sqe, &req);
        ++pending_sqes;
        maybe_flush();
    }
};

#endif //BARCH_URING_CONTEXT_H