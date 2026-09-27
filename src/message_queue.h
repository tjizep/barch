//
// A named, file-backed message queue - TODO 366.
//
#ifndef BARCH_MESSAGE_QUEUE_H
#define BARCH_MESSAGE_QUEUE_H

#include "configuration.h"
#include "queue_file.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace barch::mq {
    /**
     * One message, as it comes back out.
     *
     * `attempts` is how many deliveries of this message have already failed,
     * counting ones that started before a restart and never finished - see the
     * note on `queue` below.
     */
    struct message {
        uint64_t sequence{0};
        uint32_t attempts{0};
        std::string data;
    };

    /** how decoding a stored message went */
    enum class decoded { ok, too_short, bad_version, bad_checksum };
    const char* describe(decoded d);

    /**
     * A named queue senders publish to and one consumer drains.
     *
     * What it is for is worth stating before the API: the whole point is that a
     * message which has been accepted is still there after the process dies, so
     * every design choice here is about that and not about throughput.
     *
     * ### Peek, then handle, then remove
     *
     * `peek` reads the eldest message without taking it, and `remove` drops it.
     * They are separate because that is what makes a crash survivable: a
     * handler that dies half way leaves the message where it was, and the next
     * start hands it over again. Taking the message first and then calling the
     * handler would lose it. So the guarantee is at-least-once, never
     * exactly-once, and `message::sequence` is there so a handler that cares
     * can recognise one it has already dealt with.
     *
     * This is not a transaction and does not pretend to be. The queue is its
     * own file with its own header write, so removing a message and whatever
     * the handler wrote to a key space cannot commit together. A queue that
     * claimed otherwise would be promising something the format cannot do.
     *
     * ### Attempts, and where they're kept
     *
     * A message that always fails would otherwise be retried forever, so after
     * `max_attempts` it goes to the dead letter queue - a second queue file
     * next to the first.
     *
     * The count can't live in the message's own record: bumping it would mean
     * rewriting the message, which appends it at the tail and reorders the
     * queue. It can't live only in memory either - TODO 511. A handler that
     * takes the process down never gets to report, so nothing was counted, and
     * the message came back at attempt 0 on every start: a crash loop, and the
     * dead letter queue never saw it.
     *
     * So `started` appends a marker record to the same file before a handler
     * runs: "delivery N of message S began". It goes through the same header
     * write and the same sync policy as a message. On open, a delivery the file
     * shows as started and not finished counts as a failed attempt, and a head
     * message already at `max_attempts` is dead lettered instead of handed out
     * again. Markers aren't messages: `peek` drops them when they reach the
     * head, `size` doesn't count them, and nothing is handed a marker.
     *
     * ### Sequences never go backwards
     *
     * The next sequence is carried on from the highest one in the file. A
     * queue that drained to empty used to start again at 1, so a handler that
     * dedupes on the sequence dropped new messages as repeats - TODO 511. Every
     * marker also carries the highest sequence handed out so far, and the last
     * record in the file is never dropped: when the last message goes, a marker
     * goes in first.
     *
     * ### Thread safety
     *
     * `queue_file` is not thread safe, so everything here takes one mutex -
     * publishers and the consumer will collide otherwise. Same arrangement as
     * `aof::log`, and for the same reason.
     *
     * ### The record
     *
     * A 16 byte header, little-endian, then the payload:
     *
     *     0  4  crc32c over every byte after this field
     *     4  1  version: 1 for a message, 2 for a marker
     *     5  3  reserved, written as zero
     *     8  8  sequence (of the message a marker is about)
     *     16 .. the message; for a marker, a u32 count of deliveries started
     *           and a u64 highest sequence handed out
     *
     * A build from before markers reads version 2 as a record it doesn't know,
     * logs it and drops it. It never hands one to a handler as a message.
     *
     * The checksum is not decoration. Below `each_add` nothing orders the
     * element's bytes against the queue header that points at them, so a crash
     * can leave a record whose bytes never arrived - which is exactly what a
     * torn tail is. Without a per record check every durability level below
     * `each` would be a promise barch could not keep.
     */
    class queue {
        mutable std::mutex mut;

    public:
        /** how many times a message is handed over before it is dead lettered */
        static constexpr uint32_t default_max_attempts = 5;

        /**
         * `path` is the queue file; the dead letter queue is `path + ".dead"`
         * and is not created until something is actually dead lettered.
         */
        queue(const std::string& path, sync_policy sync,
              uint32_t max_attempts = default_max_attempts);
        ~queue();

        queue(const queue&) = delete;
        queue& operator=(const queue&) = delete;

        /** add a message to the tail. returns the sequence it was given */
        uint64_t publish(const std::string& data);

        /**
         * The eldest message, with its attempt count as it stands *before* this
         * delivery. False when the queue is empty.
         *
         * A message whose record does not verify is dropped here rather than
         * handed over, and says so in the log: a torn tail is not a message and
         * cannot be retried into existence.
         */
        [[nodiscard]] bool peek(message& into);

        /**
         * A delivery of `m` is about to start. Appends a marker, so a restart
         * that finds it unfinished counts it as a failure. Call it after `peek`
         * and before the handler runs; it throws when the file can't take it,
         * and then the handler mustn't run.
         */
        void started(const message& m);

        /** drop the eldest message - call it once the handler has succeeded */
        void remove();

        /**
         * The handler failed. Counts the attempt, and when the count reaches
         * `max_attempts` moves the message to the dead letter queue and drops
         * it from this one. Returns true when it was dead lettered.
         */
        bool failed(const message& m);

        /** push what has been written to the device, whatever the policy says */
        void sync() const;

        [[nodiscard]] uint32_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] uint64_t file_bytes() const;
        [[nodiscard]] uint64_t unsynced_bytes() const;
        /** how many are in the dead letter queue, 0 when there is not one */
        [[nodiscard]] uint32_t dead_size() const;
        /** the eldest dead lettered message, false when there are none */
        [[nodiscard]] bool peek_dead(message& into) const;
        /** drop the eldest dead lettered message */
        void remove_dead();

        /**
         * A redeclaration changed the queue - TODO 516. Both act on this object,
         * so there's never a second queue on the same path: it would have its
         * own idea of the head, and opening one can cut the file back.
         */
        void set_max_attempts(uint32_t n);
        /** closes and reopens the file (and the dead letter file) with `sync` */
        void set_policy(sync_policy sync);
        /** the queue file's path */
        [[nodiscard]] const std::string& where() const { return path; }

    private:
        std::string path;
        sync_policy policy{};
        uint32_t max_attempts{default_max_attempts};
        std::unique_ptr<queue_file> file;
        mutable std::unique_ptr<queue_file> dead;   // made on first use
        uint64_t sequence{1};
        /** sequence -> attempts so far, only for messages still in the queue */
        std::unordered_map<uint64_t, uint32_t> attempts;
        /** how many records in `file` are markers rather than messages */
        uint32_t markers{0};

        /** all of these want `mut` held */
        queue_file& dead_file() const;
        [[nodiscard]] bool peek_locked(message& into);
        void remove_locked();
        void append_marker_locked(uint64_t about, uint32_t started);
        /** markers at the head, all but a last one that holds the sequence */
        void skip_markers_locked();
        /** the head, leaving a marker behind when it's the last record */
        void drop_head_locked();
        void dead_letter_locked(const message& m, uint32_t tried, const char* why);
    };

    /**
     * A durability setting as a queue file policy.
     *
     * Here rather than in configuration.h because this is where both halves are
     * known - configuration has no business knowing what a sync_policy is - and
     * in one place rather than two because the same switch written twice is how
     * the copies in fs_api and function_sync started. See DONE 341.
     */
    sync_policy policy_of(const aof_sync_setting& setting);

    /** encode and decode one stored message, exposed so a test can corrupt one */
    void encode(uint64_t sequence, const std::string& data, std::vector<uint8_t>& into);
    decoded decode(const uint8_t* data, size_t size, message& into);
}

#endif //BARCH_MESSAGE_QUEUE_H
