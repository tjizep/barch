# Keeping barch current from Postgres

The image ships Debezium Server and a barch package that keep barch in step
with a Postgres database. Debezium is off until you set it up.

- `debezium-server` reads changes from the Postgres write-ahead log (WAL) over
  logical replication, with the built-in `pgoutput` plugin. Postgres needs no
  extension. Its Redis sink sends each change to barchd with XADD, logged in
  as the `cdc` user that first boot made.
- XADD comes from barchex STREAMS. It forwards each change for the stream
  `barch.<schema>.<table>` to the barch queue `debezium`, and keeps nothing in
  the stream (`NOSTORE`).
  The queue hands each change to `CDCAPPLY`, which updates the key.

```
Postgres WAL --pgoutput--> debezium-server --XADD, AUTH cdc--> barchd: STREAMS --> queue debezium --> CDCAPPLY --> keys
```

The package is the git repository `/opt/barch/debezium-sink`. First boot points
barchd's git sync at it (`git/repositories/debezium`), declares the queue, and
runs `XQUEUE ADD barch.* debezium NOSTORE`. If you change
`debezium.source.topic.prefix`, run that again as admin with the new prefix. The package loads into the key space
`cdc` and holds:

- `luau/cdcapply.luau`: the queue handler. It writes the keys in the global
  space, the one a connection starts in.
- `config/cdc.json`: how rows map to keys.

## What happens to a key

Each table has a key template, such as `order:{order_id}`. `{schema}`, `{table}`
and any column of the row can be used in it. Each table also has a mode:

| Change | `invalidate` (default) | `upsert` |
| --- | --- | --- |
| INSERT, UPDATE | DEL the key | SET the key to the row as JSON |
| DELETE | DEL the key | DEL the key |
| UPDATE that changes the key | DEL the old key, then as above | DEL the old key, then as above |
| TRUNCATE | DEL every key under the template's fixed prefix | same |

`invalidate` suits a cache the application fills on a miss. The next read loads
the row fresh. `upsert` keeps the row itself in barch, which then needs no load.

A TRUNCATE needs a template that starts with fixed text, such as `order:{id}`.
With `{id}:order` there is no prefix to delete under, and the keys are left.

A change that isn't JSON is dropped and counted in the key `cdc:dropped`.

## Guarantees

- Changes are applied in WAL order, one at a time. The queue hands out one
  change at a time, from the head.
- XADD answers only after the change is on the queue. The queue writes each
  change to disk (`durability = "each"`) in `/var/lib/barch/queues`. If barchd
  is down, Debezium sends the batch again.
- A change can be applied twice after a restart. DEL and SET give the same
  result either way.
- A change that fails stays at the head, and the changes behind it wait. See
  Problems.
- Debezium saves its WAL position in `/var/lib/barch-cdc/offsets.dat`. The
  replication slot keeps WAL on the database until Debezium has read it.

## 1. Prepare Postgres

On RDS or Aurora, set `rds.logical_replication = 1` in the DB parameter group
and reboot the database. On your own Postgres, set `wal_level = logical` and
restart.

Make a user for Debezium:

```sql
CREATE USER barch_cdc WITH PASSWORD 'choose-one' REPLICATION;  -- own Postgres
GRANT rds_replication TO barch_cdc;                            -- RDS or Aurora instead of REPLICATION
GRANT SELECT ON public.orders TO barch_cdc;
-- Debezium makes the publication. It must own the tables, or make it yourself:
CREATE PUBLICATION barch_cdc FOR TABLE public.orders;
```

Each table needs a primary key, so that DELETE carries the key columns. A key
template that uses columns outside the primary key needs
`ALTER TABLE ... REPLICA IDENTITY FULL`.

Allow TCP 5432 from this instance in the database's security group.

## 2. Set up Debezium

In `/etc/barch/debezium.properties`, set:

- `debezium.source.database.hostname`, `port`, `dbname`, `user`.
- `debezium.source.table.include.list`: the tables, as `schema.table`.

Put the password in `/etc/barch/debezium.env`:

```
DEBEZIUM_SOURCE_DATABASE_PASSWORD=choose-one
```

The `cdc` password comes from `/etc/barch/cdc-password`. Leave it there.

## 3. Set up the keys

Edit `config/cdc.json` in the package and commit it:

```json
{
  "defaults": { "key": "{table}:{id}", "mode": "invalidate", "ttl": 0 },
  "tables": {
    "public.orders": { "key": "order:{order_id}", "mode": "upsert", "ttl": 3600 }
  }
}
```

A table is found as `schema.table`, then as `table`, then falls back to
`defaults`. `ttl` is in seconds; 0 keeps the key. With no `key` in `defaults`,
tables not listed are skipped.

```
cd /opt/barch/debezium-sink
sudo -u barch nano config/cdc.json
sudo -u barch git commit -am "orders mapping"
redis-cli -p 14000 --user admin --pass PASSWORD FUNCTIONS SYNC debezium
```

To keep the package in your own git repository, point the setting there, as
admin, then sync:

```
configuration:SET git/repositories/debezium/url https://github.com/you/barch-sink.git
FUNCTIONS SYNC debezium
SAVE
```

## 4. Start

```
sudo systemctl enable --now debezium-server
journalctl -u debezium-server -f
redis-cli -p 14000 --user admin --pass PASSWORD QUEUE STATUS
```

Change a row in Postgres and check its key in barch.

## Stop and remove

```
sudo systemctl disable --now debezium-server
```

The replication slot stays on the database and keeps WAL there. If you stop
for good, drop it, or the database's disk fills:

```sql
SELECT pg_drop_replication_slot('barch_cdc');
DROP PUBLICATION barch_cdc;
```

## Problems

- `QUEUE STATUS` shows `waiting` rising and `last_error=CDCAPPLY: public.orders:
  column 'id' is not in the event`: the key template names a column the change
  doesn't carry. Fix `cdc.json`, commit, and run `FUNCTIONS SYNC debezium`, or
  set `REPLICA IDENTITY FULL` on the table. The next try reads the new mapping
  and the queue moves on.
- A failed change is tried again at once, not after `poll`. It runs hundreds of
  times a second and uses a CPU core. After 1,000,000 tries, about an hour, it
  goes to the dead letter queue (`dead=1` in `QUEUE STATUS`) and the changes
  behind it are applied. Keys that change was for may then be stale. Fix it
  before that, or delete those keys by hand.
- Debezium logs `not authorized`: the `cdc` user's password in
  `/etc/barch/cdc-password` no longer matches, or it lost a right. Set it again
  as admin, then `SAVE`:
  `ACL SETUSER cdc on >PASSWORD +read +write +keys +connection +function`.
- Debezium can't connect to Postgres: check the security group, `sslmode`, and
  the user's replication rights.

## What the cdc user can do

XADD is a stored function's command (barchex STREAMS), and barch needs
`+function` to run one.
Debezium's Redis client can send PING and CLIENT, which need `+connection`. So
`cdc` can
also run ECHO, CLIENT, HELLO, PUBLISH and the route commands, and run and
change stored functions. It can't run ACL, CONFIG, SAVE or FLUSHALL. barchd
listens on every address, so only the password keeps others out. Keep it on
this instance, and keep port 14000 closed in the security group to all but
your clients.

## The HTTP sink instead

The image also has barch-cdc, a small Python service that takes Debezium's
HTTP sink and applies the changes itself. It answers 200 only after barchd has
taken the change, and stops the stream at a change it can't apply, with no
retry loop. To use it:

1. In `/etc/barch/debezium.properties`, comment out the `debezium.sink.redis.*`
   lines and `debezium.sink.type=redis`, and take the `#` off the HTTP lines.
2. Set the keys in `/etc/barch/cdc.conf`: `key` and `mode` under `[defaults]`,
   and a `[table schema.name]` section for each table that differs.
3. `sudo systemctl enable --now barch-cdc debezium-server`, then
   `curl -s 127.0.0.1:14080/health`.
