//
// The queue consumer, and the one way in for publishers - TODO 366.
//
#ifndef BARCH_QUEUE_SERVICE_H
#define BARCH_QUEUE_SERVICE_H

#include <cstdint>
#include <string>

namespace barch::mq {
    /**
     * Put a message on the queue called `name`.
     *
     * `sequence` comes back as the number the message was given, which a sender
     * can hold on to: delivery is at-least-once, so a handler may see the same
     * sequence twice, and this is the end of the string that lets the two sides
     * talk about the same message.
     *
     * False, with `err` filled in, when there is no queue by that name, when it
     * has nowhere to write, or when the write itself failed. A publish that
     * could not reach the file says so rather than returning a sequence nobody
     * will ever see - a queue that quietly drops is worse than no queue.
     *
     * It wakes the consumer before it returns, so the message is handled
     * immediately rather than at the next poll. The poll only exists for
     * messages a crash left behind.
     */
    bool publish(const std::string& name, const std::string& data,
                 uint64_t& sequence, std::string& err);

    /** one line per declared queue, as FUNCTIONS QUEUES reports it */
    std::string status();

    /**
     * Arm the consumer. A no-op until a server exists to schedule on, the same
     * as cron::start - barchd calls it once at boot and server::start() calls it
     * again once there is a listener, and the second call is the one that works.
     */
    void start();
    void stop();
    /** a declaration was written or removed, so look again now */
    void request_rescan();
}

#endif //BARCH_QUEUE_SERVICE_H
