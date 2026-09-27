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
    /** disarm the consumer, waiting up to 10 s for handlers still out: for shutdown */
    void stop();
    /**
     * Disarm the consumer without waiting, for server::start and stop - TODO
     * 517. A delivery settles itself on the queue whether or not its consumer
     * is still there, and the claim that keeps a second copy from being handed
     * out lives in the queue registry, so a restart has nothing to wait for.
     * It used to wait up to 10 s holding srv_mut.
     */
    void stop_no_wait();
    /** a declaration was written or removed, so look again now */
    void request_rescan();

    /**
     * Push every open queue whose durability is `timer` to the device - TODO
     * 515. `timer` means on_demand to the queue file, and nothing asked, so a
     * timer queue was never synced at all. Called from the maintenance tick,
     * where the change log's own timer sync is, and does the work at most once
     * a poll interval however many spaces call it.
     */
    void sync_timer_queues();
}

#endif //BARCH_QUEUE_SERVICE_H
