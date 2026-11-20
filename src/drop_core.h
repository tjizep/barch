#ifndef BARCH_DROP_CORE_H
#define BARCH_DROP_CORE_H
/*
 * The rules of a dropped space's files and holders - TODO 945.
 *
 * KSPACE DROP takes a space out of the registry. Anything that held it still does: a
 * connection that chose it with USE, a park, a call in flight. The next open of the
 * name builds a new space from the files on disk. A shard deletes its files by name,
 * not by the space it belongs to.
 *
 * The decisions are here, from plain values. key_space.cpp counts the opens of a
 * dropped name, keyspace_api.cpp deletes the files at the drop, shard.cpp at the last
 * release, and asio_resp_session.h moves a connection to the space under its name.
 *
 * This header includes no locks and no clock, and TestCoreBoundary fails if it starts
 * to. test/droptest.cpp runs a connection and the name against every order of writes,
 * saves, drops, opens, commands and closes.
 */
#include <cstdint>

namespace barch::drop_core {
    /*
     * The drop deletes the files there and then. Before TODO 945 only the last holder
     * did, when it let go. Until then the files stayed, and an open of the name loaded
     * the dropped keys back.
     */
    inline bool deletes_at_drop() {
        return true;
    }

    /*
     * Whether the last holder of a dropped space deletes the files under its name. Only
     * if nothing opened the name since the drop. Then the files are a late write of the
     * dropped space's own. An open since means the files are the new space's. Before
     * TODO 945 the holder deleted them anyway: a connection that chose the space with
     * USE deleted a new space's saved keys when it closed.
     */
    inline bool deletes_at_release(uint64_t opens_at_drop, uint64_t opens_now) {
        return opens_now == opens_at_drop;
    }

    /*
     * Whether a connection whose space went runs its next command in the space under
     * the name now. Before TODO 945 it ran it in the space that went: a write landed
     * where nobody reads, and a BLPOP parked where no push reaches. Not inside an open
     * transaction: its overlay sits on the space it began in, and its commit or abort
     * ends it.
     */
    inline bool follows_name(bool space_gone, bool in_transaction) {
        return space_gone && !in_transaction;
    }
}

#endif
