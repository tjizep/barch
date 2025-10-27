#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Weffc++"
#include <moodycamel/concurrentqueue.h>
#include <moodycamel/blockingconcurrentqueue.h>
#pragma GCC diagnostic pop
#include "queue_server.h"

#include "sastam.h"
#include "art.h"
#include "thread_pool.h"
#include "logical_allocator.h"
#include "keyspec.h"
#include "module.h"
#include "value_type.h"
#include "rpc/server.h"

#include "statistics.h"
#include "circular_queue.h"
// TODO: reduce queue memory use when idle
// TODO: switch automatically between insert ordered modes
// TODO: switch queueing off from config

enum {
    retain_insert_order = 0,
};
bool is_queue_server_initialized() {
    return true;
}
class queue_server {
    struct instruction {

        instruction() = default;
        instruction(const instruction&) = default;
        instruction(instruction&&) = default;
        instruction& operator=(const instruction&) = default;
        heap::small_vector<uint8_t,32> key{};
        heap::small_vector<uint8_t,48> value{};
        barch::shard_ptr  t{};
        art::key_options options{};
        bool executed = false;
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
        [[nodiscard]] bool ordered() const {
            return true; // (t->last_queue_id+1 == qpos);
        }
        bool exec(size_t unused(tid)) {
            try {
                ++statistics::queue_processed;
                if (!t) {
                    ++statistics::queue_failures;
                    return true;
                }
                if (!ordered()) {
                    //art::std_err("queue not ordered");
                }
                t->dec_queue_size();
                //t->last_queue_id = qpos;
                t->opt_insert(options, get_key() ,get_value(),true,[](const art::node_ptr& ){});
                executed = true;
                return true;


            }catch (std::exception& e) {
                barch::std_err("exception processing queue", e.what());
            }
            ++statistics::queue_failures;
            return false;
        }
    };
    public:
    queue_server() {
        start();
    }
    ~queue_server() {
        stop();
    }
    thread_pool threads{barch::get_shard_count().size()};
    //typedef moodycamel::ConcurrentQueue<instruction> queue_type;
    typedef circular_queue<instruction> queue_type;
    struct queue_data {
        explicit queue_data(size_t size) : queue(size) {}
        queue_data() = default;
        queue_data(const queue_data&) = delete;
        queue_data(queue_data&&) = delete;
        queue_data& operator=(const queue_data&) = delete;
        queue_data& operator=(queue_data&&) = delete;
        queue_type queue{};
        moodycamel::LightweightSemaphore semaphore{0};
        moodycamel::LightweightSemaphore consumer{0};
        std::atomic<int64_t> consumers{0};
        uint64_t execs{};
        heap::vector<instruction> instructions{};
        void signalConsumers() {
            // the consumers really is just a hint of how many (consumers) there are
            // it's not precise it just aims to reduce semaphore waits and dequeue loops
            consumer.signal(consumers.load());
        }
        bool dequeue_instructions(size_t qix) {
            size_t count = 0;

            instructions.clear();
            queue.try_dequeue_all([&](instruction &ins)  {
                instructions.emplace_back(ins);
            },32);

            for (auto& ins : instructions) {
                write_lock release(ins.t->get_latch());
                ++execs;
                ins.exec(qix + 1);
                ++count;
            }

            signalConsumers();
            return count > 0;

        }
    };
    heap::vector<std::shared_ptr<queue_data>> queues{threads.size()};
    bool started = false;
    void start() {
        if (started) return;
        started = true;
        for (auto& q : queues) {
            q = std::make_shared<queue_data>(1000);

        }
        threads.start([this](size_t id) {
            auto q = queues[id];

            while (started) {
                if (!q->dequeue_instructions(id))
                    q->semaphore.wait(10000);
            }
        });
    }
    void stop() {
        if (!started) return;
        started = false;
        threads.stop();
        consume_all();
    }
    void queue_insert(const barch::shard_ptr& t, art::key_options options,art::value_type k, art::value_type v) {

        size_t at = t->get_shard_number() % threads.size();
        instruction ins;
        ins.set_key(k);
        ins.set_value(v);
        ins.options = options;
        ins.t = t;
        t->inc_queue_size();
        queues[at]->queue.enqueue(ins);

        ++statistics::queue_added;
    }
    void consume_all() {
        if (!started) return;
        for (auto& q : queues) {
            instruction ins;
            while (q->queue.try_dequeue(ins)) {
                ins.exec(1);
            }
        }
    }

    void consume(barch::shard_ptr shard) {
        auto qix = shard->get_shard_number() % threads.size();
        auto q = queues[qix];
        auto t = shard;
        if (t->get_queue_size() > 0) {
            ++q->consumers;
            q->semaphore.signal();
            q->consumer.wait(1000);
            --q->consumers;
        }
        while (t->get_queue_size() > 0) {
            ++q->consumers;
            q->consumer.wait(1000);
            --q->consumers;
        }

    }
};

std::shared_ptr<queue_server> server;

void queue_consume(barch::shard_ptr shard) {
    if (is_queue_server_running())
        server->consume(shard);
}
void queue_consume_all() {
    server->consume_all();
}
void start_queue_server() {
    server = std::make_shared<queue_server>();
}
void clear_queue_server() {
    if (server) {
        server = nullptr;
        start_queue_server();
    }
}
void stop_queue_server() {
    server = nullptr;
}

bool is_queue_server_running() {
    return server != nullptr && server->started;
}


void queue_insert(const barch::shard_ptr& shard, art::key_options options,art::value_type k, art::value_type v) {
    auto t = shard;
    if (server)
        server->queue_insert(t,options,k,v);
    else
        t->opt_insert(options,k,v,true,[](const art::node_ptr& ){});
}

