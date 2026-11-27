-- The Debezium sink for barch.
--
-- Debezium Server's Redis sink writes each change with XADD. XADD comes from
-- barchex STREAMS, which forwards each entry to the barch queue "debezium"
-- (XQUEUE ADD barch.* debezium NOSTORE). This package holds the queue handler, which
-- applies each change to the cache, one at a time, in the order they came.
--
--   luau/cdcapply.luau   the queue handler
--   config/cdc.json      how rows map to keys
--
-- It loads into the key space "cdc": barchex owns the global space, and a
-- space takes one repository. CDCAPPLY writes the keys in the global space.
--
-- A package can't write the configuration space, so the queue is declared
-- apart from it, once, as configuration:queues/debezium. The AMI's first boot
-- does that, from /opt/barch/share/debezium-queue.luau. See /opt/barch/CDC.md.

function setup()
    return {
        spaces = { cdc = {} },
        load = {
            { path = "luau",   space = "cdc" },
            { path = "config", space = "cdc" },
        },
    }
end
