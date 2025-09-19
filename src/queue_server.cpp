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
#include "rpc/server.h"

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
        size_t qpos{};
        bool operator<(const instruction& rhs) const {
            return qpos < rhs.qpos;
        }
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
        [[nodiscard]] bool exec(size_t tid) const {
            try {
                ++statistics::queue_processed;
                if (!t) {
                    ++statistics::queue_failures;
                    return false;
                }
                write_lock release(t->latch);
                if (t->last_queue_id+1 != qpos) {
                    //art::std_err("invalid queue order, expected",t->last_queue_id+1, "found", qpos, "T",tid);
                }
                --t->queue_size;
                t->last_queue_id = qpos;
                t->opt_insert(options, get_key(),get_value(),true,[](const art::node_ptr& ){});
                return true;


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
            heap::vector<instruction> instructions;
            size_t sleeps = 0;
            while (started) {
                instruction ins;
                if (q->try_dequeue(ins)) {
                    if (ins.qpos == ins.t->last_queue_id+1) {
                        ins.exec(id+1);
                    }else {
                        ++statistics::queue_reorders;
                        instructions.push_back(ins);
                    }

                }else {
                    // this is a "best effort" reordering it does not guarantee
                    // correct execution order
                    if (sleeps > 32) {
                        std::sort(instructions.begin(), instructions.end());
                        for (auto& i : instructions) {
                            i.exec(id+1);
                        }
                        instructions.clear();
                        sleeps = 0;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    ++sleeps;
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
        ins.qpos = ++t->queue_id;
        ++t->queue_size;
        queues[at]->enqueue(ins);
        ++statistics::queue_added;
    }
    void consume() {
        for (auto& q : queues) {
            instruction ins;
            while (q->try_dequeue(ins)) {
                ins.exec(1);
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
                ins.exec(shard+1);
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

