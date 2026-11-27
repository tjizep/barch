-- The queue between XADD and CDCAPPLY. First boot stores it as
-- configuration:queues/debezium. A package can't write the configuration
-- space, so it isn't in the debezium-sink package.
--
-- space is "cdc", where the debezium-sink package puts CDCAPPLY. CDCAPPLY
-- writes the keys in the global space.
--
-- dir is set here, not with CONFIG SET queue_dir: that setting is lost on a
-- restart, and this declaration is not.
--
-- A change that can't be applied stays at the head and the queue waits, so
-- the cache doesn't go wrong behind it. QUEUE STATUS says why in last_error.
-- poll only sets how often an idle queue looks for work. A failed change is
-- tried again at once, a few hundred times a second, so max_attempts at the
-- 1,000,000 cap holds it for about an hour. Then it goes to the dead letter
-- queue and the changes behind it are applied.
function transport()
    return {
        kind = "queue", name = "debezium", space = "cdc",
        call = "CDCAPPLY", user = "cdc", durability = "each",
        max_attempts = 1000000, poll = "2s",
        dir = "/var/lib/barch/queues",
    }
end
