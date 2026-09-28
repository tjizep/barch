//
// Created by teejip on 5/22/25.
//

#ifndef SERVER_H
#define SERVER_H
#include <cstdint>
#include "value_type.h"
#include <thread>
#include <utility>
#include <map>
#include <set>

#include "../art/key_options.h"
#include "variable.h"
#include "source.h"
#include "asio_includes.h"

struct caller;

namespace barch { class key_space; }
namespace barch {

    typedef std::pair<std::string, size_t> host_id;
    host_id get_host_id();
    namespace server {
        /** listen on `interface`:`port`; empty when it is listening, else why not - TODO 441 */
        extern std::string start(const std::string &interface, uint_least16_t port, bool ssl);
        extern void stop();
        /**
         * push one CLIENT INFO style line per open session, as CLIENT LIST. The session
         * vectors live in here, so the walk does too - a caller never sees a session.
         */
        extern void list_clients(caller& call);
        /**
         * The worker io_context of whichever listener is up, or nullptr when none is.
         *
         * This is the same context a session posts an asynchronous batch to, so work
         * queued here runs on the worker pool and never on a service thread. An
         * io_context is thread safe, so the pointer can be used from any thread; what
         * it must not outlive is the server, which is why nothing holds it across a
         * server::stop().
         */
        extern asio::io_context* worker_io();
    };
    namespace repl {
        struct call_result {
            int call_error{};
            int net_error{};
            [[nodiscard]] bool ok() const {
                return call_error == 0 && net_error == 0;
            }
        };
        class rpc {
        public:
            virtual ~rpc() = default;
            virtual call_result call(heap::vector<Variable>& result, const std::vector<std::string>& params) = 0;

            virtual call_result call(heap::vector<Variable>& result, const heap::vector<std::string>& params) = 0;

            virtual call_result call(heap::vector<Variable>& result, const std::vector<std::string_view>& params) = 0;

            virtual call_result call(heap::vector<Variable>& result, const heap::vector<art::value_type>& params) = 0;

            virtual call_result call(heap::vector<Variable>& result, const arg_t& params) = 0;

            virtual call_result asynch_call(heap::vector<Variable>& result, const heap::vector<art::value_type>& params) = 0;
            [[nodiscard]] virtual std::error_code net_error() const = 0;
        };
        std::shared_ptr<rpc> create(const std::string& host, int port);
        void publish(const std::string& host, int port);
        bool has_destinations();
        void call(const std::vector<std::string>& params);
        void distribute();
        void stop_repl();
        /**
         * For a clean stop: send what's queued, for up to `seconds`, and if every
         * replica has taken everything, keep this node's origin, sequence and
         * replicas for the next start to carry on from - TODO 502. Then stop.
         */
        void finish(int seconds);

        /*
         * Replicating what a shard did, not what a client asked for - TODO 498.
         *
         * The shard calls `record` at the same points it appends to the change
         * log: after the write took, under its own lock, with the result (the
         * value and flags as stored, the absolute expiry, the space, and the
         * shard it went to). So a refused write is never sent, two writes to one
         * key are queued in the order they were applied, and every command that
         * writes is covered without knowing what it means. The record is the
         * change log's own, and the replica applies it the way a replay does.
         *
         * Records go out in batches as `REPLAPPLY <origin> <first sequence>
         * <record>...`. The origin is this process, the sequence counts records
         * in queue order, so a replica can skip a batch it has already applied
         * and say when it has missed some.
         */
        /** true when a write made on this thread should be recorded: a destination
         *  exists and the write isn't one being applied for another node */
        bool capturing();
        /**
         * A write that isn't being recorded, because nothing is published. Once
         * this node has served a copy (REPLNODE), these are counted, so a replica
         * can tell that writes were made after its copy and before the stream it
         * gets when PUBLISH comes - TODO 505. Two relaxed loads otherwise.
         */
        void note_unpublished();
        /**
         * REPLNODE's answer: `<node> <incarnation> <last sequence> <unpublished>`.
         * Asking starts the counting above.
         */
        std::string copy_mark();
        /** queue one record for every destination; the sequence is assigned here */
        void record(std::string space, std::string encoded);
        /**
         * Space names as a fresh batch lists them and the positions file keeps
         * them: each as `x` and its hex, so any name, the empty one too, is one
         * field - TODO 504.
         */
        std::string encode_spaces(const std::set<std::string>& spaces);
        std::set<std::string> decode_spaces(const std::string& field);
        /** the same, with a sequence after each: `x<hex>@<seq>` */
        std::string encode_marks(const std::map<std::string, uint64_t>& marks);
        std::map<std::string, uint64_t> decode_marks(const std::string& field);
        /** writes made while one of these lives on this thread aren't recorded, so
         *  a node applying another's records doesn't send them on, or back */
        struct applying {
            applying();
            ~applying();
            applying(const applying&) = delete;
            applying& operator=(const applying&) = delete;
        };
        /*
         * Where this node is, as a replica, with each primary - TODO 502, 504.
         *
         * take_positions() is what the shard files will hold once a save that
         * starts after it has finished, and positions_saved() writes that down
         * when it has - SAVEALL calls the pair. positions_loaded() is for a LOAD
         * of a space, which may leave it behind what its primary sent, and
         * positions_retrieved() for a RETRIEVE of one from `source` (a node id,
         * empty when the source didn't say), whose copy holds that node's writes
         * up to `copy_seq`, from `incarnation`, with `unpublished` writes counted
         * (see copy_mark). Each touches only the primary the
         * space takes writes from. See REPLAPPLY in repl_api.cpp.
         */
        std::map<std::string, uint64_t> take_positions();
        void positions_saved(const std::map<std::string, uint64_t>& taken);
        void positions_loaded(const std::string& space);
        void positions_retrieved(const std::string& space, const std::string& source,
                                 const std::string& incarnation, uint64_t copy_seq,
                                 uint64_t unpublished);
        /** batches that touch `space` are held between these - TODO 505 */
        void positions_copy_begin(const std::string& space);
        void positions_copy_end(const std::string& space);
        /** this node's replication id, kept beside its data - TODO 502 */
        std::string this_node();
        /**
         * The last sequence this node has handed out. Every write numbered up to
         * it is in memory already, so a copy taken after asking holds them all -
         * TODO 504.
         */
        uint64_t last_numbered();
        struct repl_dest {
            std::string host {};
            std::string name {};
            int port {};
            size_t shard {};
            repl_dest(std::string host, int port, size_t shard) : host(std::move(host)), port(port), shard(shard) {}
            repl_dest() = default;
            repl_dest(const repl_dest&) = default;
            repl_dest(repl_dest&&) = default;
            repl_dest& operator=(const repl_dest&) = default;
        };
        std::shared_ptr<source> create_source(const std::string& host, const std::string& port, size_t shard);


        struct temp_client: repl_dest {
            temp_client() = default;
            temp_client(std::string host, int port, size_t shard) : repl_dest(std::move(host), port, shard) {}
            ~temp_client();
            /**
             * Every shard of the remote space with the same name as `ks`, as shard
             * files beside `ks`'s own - TODO 480. `user` and `secret` log in on
             * the other side, which needs read rights on the space. Nothing is
             * installed: on false, whatever arrived has been dropped.
             */
            bool receive_space(const std::shared_ptr<key_space>& ks, const std::string& user,
                               const std::string& secret, std::string& err);
            /**
             * The other side's dictionary for the space, after receive_space
             * worked; empty when it has none or is too old to send one -
             * TODO 521. The received files' compressed values need it.
             */
            std::string dictionary{};
            [[nodiscard]] bool ping() const;

        };

        struct route {
            std::string ip{};
            int64_t port{};
        };
        void clear_route(size_t shard);
        void set_route(size_t shard, const route& destination);
        route get_route(size_t shard);
    }
}



#endif //SERVER_H
