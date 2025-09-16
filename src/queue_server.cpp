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

#include "statistics.h"

class queue_server {
public:
    enum {
        into_any = 1,
        into_hash = 2
    };
private:
    struct instruction {

        instruction() = default;
        instruction(const instruction&) = default;
        instruction(instruction&&) = default;
        instruction& operator=(const instruction&) = default;
        heap::small_vector<uint8_t, 32> key{};
        heap::small_vector<uint8_t, 32> value{};
        art::tree*  t{};
        art::key_options options{};
        int into = into_any;
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
        [[nodiscard]] bool exec() const {
            try {
                ++statistics::queue_processed;
                if (!t) {
                    ++statistics::queue_failures;
                    return false;
                }
                write_lock release(t->latch);
                --t->queue_size;
                // TODO: the infernal thread_ap should be set before using any t functions
                if (into == into_hash) {
                    t->hash_insert(options, get_key(),get_value(),true,[](const art::node_ptr& ){});
                    return true;
                }
                if (into == into_any) {
                    t->opt_insert(options, get_key(),get_value(),true,[](const art::node_ptr& ){});
                    return true;

                }
            }catch (std::exception& e) {
                art::std_err("exception processing queue", e.what());
            }
            ++statistics::queue_failures;
            return false;
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
                    ins.exec();
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
    void queue_insert(art::tree* t,art::key_options options,art::value_type k, art::value_type v, int where) {

        size_t at = t->shard % threads.size();
        instruction ins;
        ins.into = where;
        ins.set_key(k);
        ins.set_value(v);
        ins.options = options;
        ins.t = t;
        ++t->queue_size;
        queues[at]->enqueue(ins);
        ++statistics::queue_added;
    }
    void consume() {
        for (auto& q : queues) {
            instruction ins;
            while (q->try_dequeue(ins)) {
                ins.exec();
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
                ins.exec();
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

void hash_queue_insert(size_t shard, art::key_options options,art::value_type k, art::value_type v) {
    auto t = get_art(shard);
    if (server)
        server->queue_insert(t,options,k,v,queue_server::into_hash);
    else
        t->hash_insert(options,k,v,true,[](const art::node_ptr& ){});
}

void queue_insert(size_t shard, art::key_options options,art::value_type k, art::value_type v) {
    auto t = get_art(shard);
    if (server)
        server->queue_insert(t,options,k,v,queue_server::into_any);
    else
        t->opt_insert(options,k,v,true,[](const art::node_ptr& ){});
}