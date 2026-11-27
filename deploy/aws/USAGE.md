# barchd on AWS: usage

barch is a key-value server that speaks the Redis protocol. This image runs it as
the `barchd` systemd service on Ubuntu 24.04. There is one AMI for x86-64 (any
CPU with AVX2) and one for arm64 (Graviton2 or later).

## Launch

1. Pick an EC2 key pair. SSH accepts keys only. The user is `ubuntu`.
2. Use a security group that allows:
   - TCP 22 from your admin address, for SSH.
   - TCP 14000 from your application's addresses or VPC, for barchd.
   Do not open 14000 to 0.0.0.0/0. Traffic on it is not encrypted.
3. Wait for the instance to pass its status checks.

## Get the password

The first boot makes a random password for the `admin` user. It is different on
every instance. Read it over SSH:

    ssh ubuntu@INSTANCE
    sudo cat /root/barch-credentials

Change it after the first login (see below).

## Connect

    redis-cli -h INSTANCE -p 14000 --user admin --pass PASSWORD

Clients must log in with a user name and a password, as in `AUTH admin PASSWORD`.
A password alone (`redis-cli -a PASSWORD`) is refused. A client that does not log
in can run nothing but AUTH.

Change the password, then save:

    ACL SETUSER admin on >NEW-PASSWORD +all
    SAVE

Add users with fewer rights for applications:

    ACL SETUSER app on >APP-PASSWORD +read +write +keys +hash +list +orderedset

## Keep it current from Postgres

Debezium Server comes installed and off. It reads the Postgres WAL over logical
replication (pgoutput, no extension) and sends each change to barchd with XADD.
barchd queues it and DELs or SETs the row's key. Set the database in
`/etc/barch/debezium.properties` and `/etc/barch/debezium.env`, and the keys in
`config/cdc.json` in the git repository `/opt/barch/debezium-sink`. Then:

    sudo systemctl enable --now debezium-server

`QUEUE STATUS` shows the queue and its last error. The steps are in
`/opt/barch/CDC.md`.

## S3 backup and vector search

barchex is loaded into the `vectors`, `s3`, `watchdog` and `lent` key spaces.
Its docs: https://github.com/tjizep/barchex

Back up a key space to a bucket:

    configuration:SET s3.region us-east-1
    configuration:SET s3.access_key AKIA...
    configuration:SET s3.secret_key SECRET
    s3.BACKUP SAVE default my-bucket/nightly

Use an IAM user that can reach only that bucket. Any barch user with `+read`
can read the secret.

Vectors: `USE vectors`, then `vectors.SET name BUFFER` and
`vectors.CLOSEST BUFFER k`, where BUFFER is the vector as packed f32.

## Ports

- 14000/tcp: barchd, Redis protocol.
- 22/tcp: SSH.
- 14080 and 14081 on 127.0.0.1 only, if you use the HTTP sink in `CDC.md`.

No other port is open. Commands such as `HTTP START` can open more, and only a
user with the rights for it can run them.

## Data

- Data directory: `/var/lib/barch`. barchd saves there on a timer and when it stops.
- Settings: `/etc/barch/barchd.env` (bind address, port, extra options).
- Logs: `journalctl -u barchd`.
- Version: `/opt/barch/VERSION`.
- CDC: `/opt/barch/debezium-sink`, `journalctl -u debezium-server`.

For large data sets, mount a separate EBS volume at `/var/lib/barch` before the
first start, owned by `barch:barch`. Use EBS snapshots for backups. Stop the
service first, or run SAVE, so the files are complete.

## Service

    sudo systemctl status barchd
    sudo systemctl restart barchd

A stop saves every key space. A large store can take minutes to stop.

## Upgrade

In place, on the same instance:

    sudo systemctl stop barchd
    sudo cp -a /var/lib/barch /var/lib/barch.bak    # or take an EBS snapshot
    sudo install -m 755 NEW-barchd /opt/barch/bin/barchd
    sudo systemctl start barchd

Or launch the new AMI version, stop the old instance, and move the data volume
across. Read the release notes first: they say when a version changes the data
format.

## Internet access

The image needs no internet access to run. Ubuntu security updates need it.

## Support

https://github.com/tjizep/barch/issues
