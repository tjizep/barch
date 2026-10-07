//
// One Raft group on this node: its store, its state manager and its NuRaft
// server - TODO 610.
//
#ifndef BARCH_CLUSTER_RAFT_GROUP_H
#define BARCH_CLUSTER_RAFT_GROUP_H

#include "group_store.h"
#include "nuraft.hxx"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace barch::cluster {
    /*
     * A group listens on its own port, raft_port + its number, because NuRaft gives
     * each server its own listener. Its state is `<dir>/group_<n>.raft`.
     *
     * A node either starts a group (`bootstrap`): it's the only member and leads
     * at once. Or it waits to be added by the leader (`join`): it doesn't start an
     * election of its own, so it can't become the leader of a group of one that
     * nobody else is in. After the first start, the file says which it was.
     */
    class raft_group {
    public:
        struct options {
            uint32_t group{0};
            std::string dir{};
            std::string host{"127.0.0.1"};
            int base_port{0};               // raft_port; this group's is base_port + group
            int32_t server_id{0};           // only read on the first start
            bool join{false};               // only read on the first start
            int heartbeat_ms{100};
            int election_min_ms{400};
            int election_max_ms{800};
            int client_timeout_ms{3000};
            // a snapshot every this many entries; 0 is none - TODO 611
            int snapshot_distance{0};
            // entries kept behind a snapshot when the log is compacted
            int reserved_entries{0};
            // how long a leader waits on a member installing a snapshot
            int snapshot_timeout_ms{600000};
        };

        /** what became of an append */
        enum class outcome {
            committed,  // a quorum has it, and it's been applied here
            refused,    // it never reached the log: not the leader, or shutting down
            unknown     // it reached the log, and leadership changed before it committed
        };

        struct member {
            int32_t id{0};
            std::string endpoint{};
            bool learner{false};
        };

        raft_group(options o, nuraft::ptr<nuraft::state_machine> sm);
        ~raft_group();

        raft_group(const raft_group&) = delete;
        raft_group& operator=(const raft_group&) = delete;

        /** false with `err` set when the group couldn't start */
        bool start(std::string& err);
        void stop();

        [[nodiscard]] bool running() const { return srv() != nullptr; }
        [[nodiscard]] bool is_leader() const;
        /** -1 when there isn't one this node knows of */
        [[nodiscard]] int32_t leader_id() const;
        [[nodiscard]] std::string leader_endpoint() const;
        [[nodiscard]] int32_t my_id() const { return self_id; }
        [[nodiscard]] std::string my_endpoint() const { return self_endpoint; }
        [[nodiscard]] uint64_t term() const;
        [[nodiscard]] uint64_t committed_index() const;
        /** the last index in this node's log; safe inside an event callback */
        [[nodiscard]] uint64_t last_index() const;
        [[nodiscard]] uint32_t number() const { return opt.group; }
        [[nodiscard]] std::vector<member> members() const;

        /**
         * Append one entry and wait for it to commit, for up to the client timeout.
         * `index` is where it landed when it reached the log.
         */
        outcome append(nuraft::ptr<nuraft::buffer> data, uint64_t& index, std::string& why);

        /**
         * Add a member, as the leader, and wait until it's in the configuration.
         * One change at a time: NuRaft refuses a second while one is under way.
         */
        bool add_member(int32_t id, const std::string& endpoint, int wait_ms, std::string& err,
                        bool learner = false);
        /**
         * Make a learner a voter, as the leader - TODO 611. A member joins as a
         * learner so it doesn't count towards a quorum while it catches up.
         */
        bool promote(int32_t id, int wait_ms, std::string& err);
        /** the last log index a member is known to hold, as the leader sees it; 0 when not known */
        [[nodiscard]] uint64_t member_index(int32_t id) const;
        /** the data files' snapshot, and saving one - see group_store */
        void save_snapshot(nuraft::snapshot& s);
        [[nodiscard]] nuraft::ptr<nuraft::snapshot> last_snapshot() const;
        /** the first index still in the log */
        [[nodiscard]] uint64_t start_index() const;
        bool remove_member(int32_t id, int wait_ms, std::string& err);
        /** hand leadership to another member, when this node has it */
        void yield_leadership();
        /** hand leadership to that member, once it has caught up - TODO 612 */
        void hand_over(int32_t id);

        /** a hook for NuRaft's events, set before start */
        void on_event(std::function<void(nuraft::cb_func::Type, nuraft::cb_func::Param*)> f) {
            events = std::move(f);
        }

        /** the endpoint a group listens on, for a node at host:base_port */
        static std::string endpoint_for(const std::string& host, int base_port, uint32_t group);

    private:
        class state_mgr;

        // copies, taken under `ptrs`: stop() drops them while other threads ask
        // whether this node leads
        [[nodiscard]] nuraft::ptr<nuraft::raft_server> srv() const;
        [[nodiscard]] std::shared_ptr<group_store> st() const;

        options opt;
        nuraft::ptr<nuraft::state_machine> machine;
        mutable std::mutex ptrs;
        std::shared_ptr<group_store> store;            // under ptrs
        nuraft::ptr<state_mgr> smgr;                    // start and stop only
        nuraft::raft_launcher launcher;                 // start and stop only
        nuraft::ptr<nuraft::raft_server> server;        // under ptrs
        std::function<void(nuraft::cb_func::Type, nuraft::cb_func::Param*)> events;
        int32_t self_id{-1};
        std::string self_endpoint{};
        mutable std::mutex change;      // one membership change at a time
    };
}

#endif //BARCH_CLUSTER_RAFT_GROUP_H
