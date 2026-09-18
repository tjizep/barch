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
     * `attempts` is how many times this message has already been handed to a
     * handler in this process, and it is not stored in the file - see the note
     * on `queue` below.
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
     * ### Attempts are counted in memory
     *
     * A message that always fails would otherwise be retried forever, so after
     * `max_attempts` it goes to the dead letter queue - a second queue file
     * next to the first. The count lives in a map here rather than in the
     * stored record, and that is a deliberate trade: keeping it in the record
     * means rewriting the message to bump it, which appends it at the tail and
     * reorders the queue. Ordering is worth more than what this loses, and what
     * it loses, said plainly rather than hidden: a restart forgets the counts,
     * so a poison message gets `max_attempts` more tries after every restart.
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
     *     4  1  version, 1
     *     5  3  reserved, written as zero
     *     8  8  sequence
     *     16 .. the message
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

    private:
        std::string path;
        sync_policy policy{};
        uint32_t max_attempts{default_max_attempts};
        std::unique_ptr<queue_file> file;
        mutable std::unique_ptr<queue_file> dead;   // made on first use
        uint64_t sequence{1};
        /** sequence -> attempts so far, only for messages still in the queue */
        std::unordered_map<uint64_t, uint32_t> attempts;

        /** all of these want `mut` held */
        queue_file& dead_file() const;
        [[nodiscard]] bool peek_locked(message& into);
        void remove_locked();
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
