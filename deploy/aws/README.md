# barchd AMI for AWS Marketplace

This folder builds two Amazon Machine Images (AMIs), x86-64 and arm64, that run
barchd on Ubuntu 24.04 LTS, ready to list on AWS Marketplace. `USAGE.md` is the
buyer's guide. Its text fits the listing's 4,000-character usage field.
`CDC.md` is the guide to keeping barch current from Postgres.

## What the image does

- barchd is in `/opt/barch/bin/barchd`. The build compiles it from the `v0.5.8b`
  tag and checks that the tag is commit `d915a6ba`. The release has no barchd
  binary yet. A prebuilt tarball can be used instead (see Build).
- barch's CMakeLists.txt builds with `-march=native`. An AMI built that way on a
  c7i would use AVX-512 and die with SIGILL on CPUs without it. The build
  replaces the flag with a baseline: `x86-64-v3` (AVX2) on x86-64, and
  `armv8.2-a+crypto+fp16+rcpc+dotprod` (Graviton2) on arm64. `BARCH_MARCH` in
  `install-barchd.sh` sets another. A prebuilt tarball must be built the same
  way.
- `barchd.service` runs it as the `barch` system user. Data is in
  `/var/lib/barch`. Settings are in `/etc/barch/barchd.env`.
- barchd listens on 0.0.0.0, port 14000. Clients must log in.
- `barch-firstboot.service` runs once, before barchd. It makes a random 32-character
  password for the `admin` user, locks the `default` user with
  `ACL SETUSER default on -all +auth`, saves, and writes the password to
  `/root/barch-credentials` (mode 600). Until that is done barchd only listens on
  127.0.0.1. If it fails, barchd does not start.
- First boot also makes the `cdc` user, with
  `+read +write +keys +connection +function`, and writes its password to
  `/etc/barch/cdc-password` (mode 600, owned by `barch-cdc`). It loads the
  `debezium-sink` package (CDCAPPLY) through barchd's git sync into the key
  space `cdc`, declares the queue `debezium`, and runs
  `XQUEUE ADD barch.* debezium NOSTORE` so barchex's XADD feeds the queue.
- git is installed, because barchd's git sync runs the `git` binary.
- barchex, for S3 backup and vector search, is cloned at a pinned commit
  (`barchex_commit`) into `/opt/barch/barchex`. First boot syncs it from there
  into the `vectors`, `s3`, `watchdog` and `lent` key spaces, and STREAMS
  (XADD) into the global space. First boot runs
  before the network is up, and the image runs with no internet, so it syncs
  from the local clone, not from GitHub. origin stays GitHub. To update:
  `sudo -u barch git -C /opt/barch/barchex pull`, then `FUNCTIONS SYNC barchex`.
- Debezium Server 3.7.1 is in `/opt/debezium-server`, with Java 21. It runs as
  `barch-cdc` and is off. barch-cdc, the HTTP-sink fallback, is in
  `/opt/barch/libexec/barch-cdc.py`, also off.
  See "Debezium push" below.
- SSH accepts keys only. Root can't log in, and its password is locked.
- The image holds no password, key or credential. The build deletes Packer's SSH
  key, the host keys, cloud-init state, the machine id, shell history and logs.

## Files

| File | What it is |
| --- | --- |
| `barchd.pkr.hcl` | Packer template |
| `scripts/install-barchd.sh` | Updates Ubuntu, builds or downloads barchd, installs the service |
| `scripts/install-cdc.sh` | Installs Java, Debezium Server (hash checked), barch-cdc and the sink package as a git repository |
| `scripts/install-barchex.sh` | Clones barchex at `barchex_commit` into `/opt/barch/barchex` |
| `scripts/harden.sh` | SSH and root settings |
| `scripts/check-image.sh` | Fails the build if a rule is broken; runs first boot on scratch paths |
| `scripts/cleanup.sh` | Removes build-instance state; runs last |
| `files/barch-firstboot.sh` | The first-boot script |
| `files/*.service`, `files/barchd.env` | systemd units and settings |
| `files/00-barch-hardening.conf` | sshd settings |
| `debezium-sink/` | barch package: the CDCAPPLY queue handler, `config/cdc.json`. Installed as the git repository `/opt/barch/debezium-sink` |
| `files/debezium-queue.luau` | The queue declaration first boot stores as `configuration:queues/debezium` |
| `files/debezium-run.sh` | Starts Debezium Server with the `cdc` password in its environment |
| `files/barch-cdc.py` | HTTP-sink fallback: applies Debezium changes to barchd |
| `files/cdc.conf` | Row-to-key mapping for barch-cdc only |
| `files/debezium.properties`, `files/debezium.env` | Debezium Server settings and its password |
| `files/barch-cdc.service`, `files/debezium-server.service` | CDC units, not enabled |
| `USAGE.md` | Usage instructions for the listing |
| `CDC.md` | Postgres CDC guide, installed as `/opt/barch/CDC.md` |

## Build

You need Packer 1.10 or later and AWS credentials for the seller account, in
us-east-1. The build costs a little: a c7i.2xlarge and a c7g.2xlarge for about
15 minutes each, two 16 GiB snapshots, and data transfer.

```bash
packer init deploy/aws
packer validate deploy/aws
packer build deploy/aws
```

One architecture only:

```bash
packer build -only='amazon-ebs.amd64' deploy/aws
packer build -only='amazon-ebs.arm64' deploy/aws
```

The AMI ids are in `deploy/aws/packer-manifest.json`. The build reaches GitHub
for the barch source and its pinned dependencies, Ubuntu's mirrors for packages,
Maven Central for Debezium Server (747 MB), and GitHub for barchex. The Debezium tarball's sha256 is
pinned in `debezium_sha256`. For another version, set `debezium_version` and the
sha256 Maven Central publishes next to the tarball.

With a prebuilt binary, from a tarball that has `barchd` at its top level:

```bash
packer build \
  -var barchd_tarball_url=https://github.com/tjizep/barch/releases/download/v0.5.8b/barchd-ubuntu24-x86_64.tar.gz \
  -var barchd_tarball_sha256=<sha256> \
  deploy/aws
```

That asset doesn't exist yet. The CI release job would have to attach it.

For a new version, set `barch_version`, `barch_ref` and `barch_commit`. For a
newer barchex, set `barchex_commit` to its full hash.

## Marketplace rules this covers

From the AWS Marketplace AMI product policies, checked 27-11-2026:

| Rule | Where it is met |
| --- | --- |
| Source AMI in us-east-1 | `region` default |
| HVM, EBS-backed, x86-64 or arm64 | Ubuntu 24.04 HVM gp3 base, one AMI each |
| Snapshots not encrypted | `encrypt_boot = false` |
| IMDSv2 only | `imds_support = "v2.0"` |
| Supported OS, patched, no high or critical CVEs | Latest Canonical 24.04 AMI plus `full-upgrade` at build |
| No password SSH | `PasswordAuthentication no` |
| No password root login | `PermitRootLogin no`, root locked |
| No authorized keys in the image | `cleanup.sh` deletes them all |
| No hardcoded secrets | Password made per instance on first boot |
| Full sudo for the buyer | Default `ubuntu` user |
| SSH reachable for the scan | Port 22, no host firewall |
| Usage instructions | `USAGE.md` |

The policy forbids password logins for instance services, with one exception: a
service with no other way to log in may use a strong random password per
instance, used for the first login and then changed. barchd has only password
logins, so the image uses that exception. `USAGE.md` tells the buyer to change
the password.

## Debezium push

The product sheet promises a bundled Debezium Server that keeps barch current
from the Postgres WAL. Debezium's Redis sink writes Redis streams with XADD.
barch has no streams. barchex STREAMS adds them, and forwards each new entry
to a barch queue:

```
Postgres --pgoutput--> debezium-server --Redis sink, XADD, AUTH cdc--> barchd: barchex STREAMS --XQUEUE--> queue debezium --> CDCAPPLY --> keys
```

- XADD comes from barchex STREAMS, in the global space. First boot runs
  `XQUEUE ADD barch.* debezium NOSTORE` there, after the queue is declared.
  Debezium names each stream `barch.<schema>.<table>`, from `topic.prefix`.
  XADD gives the entry an id and puts `{stream, id, fields}` on the queue.
  `NOSTORE` keeps it out of the stream, so the streams stay empty. XADD fails
  if the queue won't take it, and Debezium sends the batch again. The queue writes each change
  to disk (`durability = "each"`).
- `luau/cdcapply.luau` is the queue's handler. It is in the key space `cdc`:
  barchex owns the global space, and barch lets one repository load into a
  space. It writes the keys in the global space through `barch.space["0"]`. Each table has a key template
  (`order:{id}`) and a mode. `invalidate` runs DEL. `upsert` runs SET with the
  row as JSON, with an optional TTL. DELETE runs DEL in both. An UPDATE that
  changes the key deletes the old key too. TRUNCATE deletes the keys under the
  template's fixed prefix, in batches found with the space handle's `range` (a
  script can't call `RANGE`, which is asynchronous). A message that isn't JSON is
  dropped and counted in `cdc:dropped`.
- The mapping is `config/cdc.json` in the package. The buyer commits a change
  and runs `FUNCTIONS SYNC debezium`, or points the setting at their own repo.
- The queue declaration is not in the package: a package can't write
  `configuration`. First boot stores it with
  `configuration:SETF`. Its `dir` is in the declaration because
  `CONFIG SET queue_dir` is lost on a restart.
- The queue applies one change at a time, from the head, so changes apply in
  WAL order. A change that fails stays at the head and the rest wait.
- Debezium takes no snapshot (`snapshot.mode=no_data`). A cache needs changes,
  not a copy of the database.
- Debezium is not enabled. The buyer sets the database and enables it. The
  image holds no database address or password.

barch-cdc, the first version of this, stays in the image as a fallback for
Debezium's HTTP sink. `CDC.md` says how to switch.

### Known limits

- A failed change is retried at once, not after the queue's `poll`. Measured
  here: about 290 tries a second, one core busy. `max_attempts` is capped at
  1,000,000, so after about an hour the change goes to the dead letter queue
  and the changes behind it apply. Fixing that needs a backoff in barch's
  queue service (`src/queue_service.cpp`), not in this folder.
- `+function` and `+connection` give `cdc` more than XADD: ECHO, CLIENT, HELLO,
  PUBLISH, the route commands, and running and changing stored functions. ACL,
  CONFIG, SAVE and FLUSHALL stay refused. barch has no right for one function.
- barchex S3 takes static keys from `configuration` only. barch has no
  per-space ACL, so any user with `+read`, `cdc` included, can read
  `s3.secret_key`. `cdc` can't use the key: it lacks `outbound`. An EC2
  instance role (IMDSv2) would remove the stored key. That is asked of
  barchex, not done.
- A REMF of the queue declaration doesn't stop the running queue. A restart
  does. There is no command to clear a queue.

### What the product sheet claims that the code doesn't have

- Prometheus `/metrics`: barch has no such endpoint. INFO and STATS exist.
- A Docker container: there is no Dockerfile for barchd here.
- "Zero-extension Postgres WAL invalidation" is what this folder adds, but only
  in the AMI, and only through Debezium Server. barch itself has no CDC code.

The sheet's transaction blocks are `BEGINTX` / `COMMITTX` (and `ABORTTX`).
Earlier drafts said `STARTTX`, which doesn't exist.

## What needs your AWS account

None of this has been done. Each step uses your account, and some cost money.

1. Seller registration in the AWS Marketplace Management Portal. For a free
   product, only the public seller profile is needed. A paid product also needs
   the tax interview (W-9 or W-8), a bank account that takes USD, disbursement
   settings, and KYC for some regions.
2. An IAM role for AMI ingestion: IAM, Create role, AWS service, AWS
   Marketplace, "Marketplace - AMI Assets Ingestion", with the
   `AWSMarketplaceAmiIngestion` policy. You make it once.
3. The Packer build above, in us-east-1.
4. A test launch of each AMI: start an instance, read
   `/root/barch-credentials`, connect with redis-cli, check that a client without
   a login is refused, and check that a second instance gets a different password.
   Launch the x86-64 AMI on an AMD type (m7a) and the arm64 AMI on a Graviton2
   type (t4g), the oldest CPUs the baseline allows.
5. A CDC test: an RDS Postgres with `rds.logical_replication = 1`, then the
   steps in `CDC.md`. Change, delete and truncate rows and check the keys.
6. The scan, once per AMI: in the portal, Products, Server, the product, Delivery, Versions,
   Add version, "Test this version". It scans for unpatched CVEs and policy
   problems, and changes nothing a buyer sees. Give the AMI id, the role ARN, OS
   Ubuntu 24.04, user name `ubuntu`, scanning port 22.
7. The listing form: title, short and long description, logo, highlights,
   categories, pricing (free or hourly), EULA, support details, the text of
   `USAGE.md`, and these security group rules:
   - TCP 22 from the buyer's admin CIDR.
   - TCP 14000 from the buyer's application CIDR.
8. Submit for review. Automated checks can pass in hours. A flagged listing
   takes 3 to 5 business days.

Free products need a support process and must keep the software free of known
vulnerabilities. AMIs older than two years are not allowed, so rebuild on each
release and at least once a year for OS patches.

## Tested here

`barch-firstboot.sh` was run against a local barchd 0.5.8 as an ordinary user.
After it ran, a client without a login got "not authorized" for PING, GET, SET,
KEYS, CONFIG, ACL, SAVE, FLUSHALL, INFO, DBSIZE and HTTP START. The admin login
worked, a wrong password was refused, and both held after a restart. A rerun on a
store that was already locked reused the saved password and passed. barchd opened
only its one port.

First boot with the `cdc` user and the sink package was run the same way,
fresh and as a rerun, on scratch paths. It loaded CDCAPPLY and barchex
STREAMS (a scratch commit of its uncommitted tree) through git sync, declared
the queue, added the forward with `NOSTORE` (1 fresh, 0 on the rerun, 1 on a
store whose forward had no `NOSTORE`), and its probes passed: the `cdc` user could PING and
EXISTS and was refused FLUSHALL.

The package was tested end to end on a local barchd 0.5.8, logged in as `cdc`
with redis-py, sending XADD the way the Redis sink sends it in its default
compact format: PING, then a pipelined batch of 400 (400 ids, rising, applied
in order). It covered insert, update, upsert with the row as JSON and a TTL,
update that moves the key, delete, invalidate through `defaults`, a tombstone
(skipped), a message that isn't JSON (dropped and counted), a schema envelope,
`MAXLEN ~ n`, a TRUNCATE over more keys than one batch that left `orderx`
alone, and refusals of bad field lists. A missing
column stopped the queue: `last_error` named the column and the change behind
it waited. It passed again against the store first boot made, with the
passwords first boot made. A commit to `cdc.json` was picked up by
`FUNCTIONS SYNC debezium`. Not tested: the extended message format, a queued
change across a barchd restart, and the queue moving on after the mapping is
fixed. With barchex STREAMS in place of the package's own XADD, the same test
passed again, run against a fresh first boot. With `NOSTORE` the stream was
empty afterwards (XLEN 0). The same runs passed on a clone of barchex
`6524a98` (main, with STREAMS) from GitHub, set up as `install-barchex.sh`
sets it up, and a `git pull` in that clone worked.

First boot with barchex (a clone of `095756a`) passed fresh and as a rerun.
With an empty package in its place, first boot failed at the barchex probe.
On the store it made, `s3.SIGV4` passed its self-test, `vectors.SET` and
`vectors.CLOSEST` returned the nearest points, and both still worked after a
restart. With fake keys and a closed endpoint, admin's `s3.S3 BUCKETS` failed
to connect, and `cdc`'s was refused for lack of `outbound`. S3 against a real
bucket was not tested.

barch-cdc was run against a local barchd 0.5.8 logged in as `cdc`, with events
shaped as Debezium sends them: insert, snapshot read, update, update that moves
the key, delete, invalidate, a schema envelope, a CloudEvent, an array, a table
with no key (skipped), a missing column (500), bad JSON (logged and dropped),
and a TRUNCATE over 2,500 keys that left `orderx` alone. With barchd stopped it
answered 503 and logged it. After barchd started again the same event gave 200
and the key was there.

Not tested: the Packer template (no Packer or AWS here), the scripts that change a
real Ubuntu system, the Marketplace scan, Debezium Server itself (its Redis sink
and Jedis client have never talked to barchd; Jedis may send commands such as
`CLIENT SETINFO` the test didn't), a real Postgres, and any arm64 or `-march=x86-64-v3` build of barchd. No arm64 compiler
is installed here. barch uses sse2neon for NEON, but that path has no CI job.
