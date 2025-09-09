#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Weffc++"
#include <moodycamel/concurrentqueue.h>
#pragma GCC diagnostic pop
#include "sastam.h"
#include "art.h"
#include "thread_pool.h"
#include "logical_allocator.h"
#include "keyspec.h"
#include "module.h"
#include "value_type.h"
#include "server.h"

class queue_server {
    struct instruction {
        instruction() = default;
        instruction(const instruction&) = default;
        instruction(instruction&&) = default;
        instruction& operator=(const instruction&) = default;
        heap::small_vector<uint8_t, 32> key{};
        heap::small_vector<uint8_t, 32> value{};
        art::tree*  t{};
        art::key_options options{};
        void set_key(art::value_type k) {
            key.append(k.to_view());
        }
        void set_value(art::value_type v) {
            value.append(v.to_view());
        }
        [[nodiscard]] art::value_type get_key() const {
            return {key.data(), key.size()};
        }
        [[nodiscard]] art::value_type get_value() const {
            return {value.data(), value.size()};
        }

    };
    public:
    queue_server() {

    }
    ~queue_server() {
        stop();
    }
    thread_pool threads{art::get_shard_count().size()};
    typedef moodycamel::ConcurrentQueue<instruction> queue_type;
    heap::vector<std::shared_ptr<queue_type>> queues{threads.size()};
    bool started = false;
    void start() {
        started = true;
        for (auto& q : queues) {
            q = std::make_shared<queue_type>(1000);
        }
        threads.start([this](size_t id) {
            auto q = queues[id];
            while (started) {
                instruction ins;
                if (q->try_dequeue(ins)) {
                    try {
                        write_lock release(ins.t->latch);
                        --ins.t->queue_size;
                    // TODO: the infernal thread_ap should be set before using any t functions
                        ins.t->opt_insert(ins.options, ins.get_key(),ins.get_value(),true,[](const art::node_ptr& ){});
                    }catch (std::exception& e) {
                        art::std_err("exception processing queue", e.what());
                    }
                }else {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
        });
    }
    void stop() {

        started = false;
        threads.stop();
        consume();


    }
    void queue_insert(art::tree* t,art::key_options options,art::value_type k, art::value_type v) {
        size_t at = t->shard % threads.size();
        instruction ins;
        ins.set_key(k);
        ins.set_value(v);
        ins.options = options;
        ins.t = t;
        ++t->queue_size;
        queues[at]->enqueue(ins);
    }
    void consume() {
        for (auto& q : queues) {
            instruction ins;
            while (q->try_dequeue(ins)) {
                write_lock release(ins.t->latch);
                --ins.t->queue_size;
                ins.t->opt_insert(ins.options, ins.get_key(),ins.get_value(),true,[](const art::node_ptr& ){});
            }
        }
    }
    void consume(size_t shard) {
        auto t = get_art(shard);
        auto q = shard % threads.size();
        instruction ins;
        // this is more complicated than it looks
        // queue_size may sometimes be a subset
        // of the actual queue size - but usually it's the same
        while (t->queue_size > 0) {
            if (queues[q]->try_dequeue(ins)) {
                write_lock r1(ins.t->latch);
                --ins.t->queue_size;
                ins.t->opt_insert(ins.options, ins.get_key(),ins.get_value(),true,[](const art::node_ptr& ){});
            }else {
                std::this_thread::yield();
            }
        }
    }
};

std::shared_ptr<queue_server> server;

void queue_consume(size_t shard) {
    if (server)
        server->consume(shard);
}
void queue_consume_all() {
    if (server)
        server->consume();
}
void start_queue_server() {
    if (!server) {
        server = std::make_shared<queue_server>();
        server->start();
    }
}
void stop_queue_server() {
    if (server) {
        server->stop();
    }
    server = nullptr;
}

bool is_queue_server_running() {
    return server != nullptr;
}

void queue_insert(size_t shard, art::key_options options,art::value_type k, art::value_type v) {
    auto t = get_art(shard);
    if (server)
        server->queue_insert(t,options,k,v);
}