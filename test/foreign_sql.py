# Start a MySQL or Postgres container for the foreign-driver tests.
# A missing docker binary, a daemon that will not talk, or a container
# that never becomes ready prints SKIP and returns None. The resolved
# DSN is never printed.
import atexit
import os
import shutil
import subprocess
import time

MYSQL_IMAGE = os.environ.get("BARCH_MYSQL_IMAGE", "mysql:8.0")
POSTGRES_IMAGE = os.environ.get("BARCH_POSTGRES_IMAGE", "postgres:16-alpine")
MYSQL_PORT = int(os.environ.get("BARCH_MYSQL_PORT", "13306"))
POSTGRES_PORT = int(os.environ.get("BARCH_POSTGRES_PORT", "15432"))
READY_SEC = int(os.environ.get("BARCH_SQL_READY_SEC", "90"))


def _run(cmd, timeout=30, env=None):
    try:
        return subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, env=env
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None


def docker_bin():
    if os.environ.get("BARCH_SKIP_DOCKER"):
        return None
    exe = shutil.which("docker")
    if not exe:
        return None
    info = _run([exe, "info"], timeout=10)
    if info is None or info.returncode != 0:
        return None
    return exe


class SqlContainer:
    def __init__(self, docker, name, dsn):
        self.docker = docker
        self.name = name
        self.dsn = dsn
        self._stopped = False

    def stop(self):
        if self._stopped:
            return
        self._stopped = True
        _run([self.docker, "rm", "-f", self.name], timeout=30)


def _rm(docker, name):
    _run([docker, "rm", "-f", name], timeout=30)


def _wait_exec(docker, name, args, exec_env=None, timeout=READY_SEC):
    deadline = time.time() + timeout
    prefix = [docker, "exec"]
    if exec_env:
        for k, v in exec_env.items():
            prefix += ["-e", "%s=%s" % (k, v)]
    prefix.append(name)
    while time.time() < deadline:
        r = _run(prefix + args, timeout=15)
        if r is not None and r.returncode == 0:
            return True
        time.sleep(0.5)
    return False


def start_mysql(name="barch-foreign-mysql", port=MYSQL_PORT):
    docker = docker_bin()
    if not docker:
        print("SKIP: docker not available")
        return None
    _rm(docker, name)
    run = _run(
        [
            docker, "run", "-d", "--name", name,
            "-e", "MYSQL_ROOT_PASSWORD=barch",
            "-e", "MYSQL_DATABASE=barch",
            "-e", "MYSQL_USER=barch",
            "-e", "MYSQL_PASSWORD=barch",
            "-p", "127.0.0.1:%d:3306" % port,
            MYSQL_IMAGE,
        ],
        timeout=180,
    )
    if run is None or run.returncode != 0:
        print("SKIP: docker mysql failed to start")
        _rm(docker, name)
        return None
    ping = [
        "mysqladmin", "ping", "-h", "127.0.0.1", "-ubarch",
        "--silent",
    ]
    if not _wait_exec(docker, name, ping, exec_env={"MYSQL_PWD": "barch"}):
        print("SKIP: docker mysql did not become ready")
        _rm(docker, name)
        return None
    seed = [
        "mysql", "-h", "127.0.0.1", "-uroot", "-e",
        "CREATE DATABASE IF NOT EXISTS fm_comp;"
        "GRANT ALL ON fm_comp.* TO 'barch'@'%';"
        "USE barch;"
        "CREATE TABLE t (k VARCHAR(255) PRIMARY KEY, v TEXT);"
        "INSERT INTO t (k, v) VALUES"
        " ('sku', 'widget'),"
        " ('o''reilly', 'quoted'),"
        " ('x''OR''1''=''1', 'safe'),"
        " ('Smith 42', 'whole');"
        "CREATE TABLE person (surname VARCHAR(255), age VARCHAR(32), name TEXT,"
        " PRIMARY KEY (surname, age));"
        "INSERT INTO person (surname, age, name) VALUES ('Smith', '42', 'Jane');"
        "CREATE TABLE fm_comp.person (surname VARCHAR(255), age VARCHAR(32), name TEXT,"
        " PRIMARY KEY (surname, age));"
        "INSERT INTO fm_comp.person (surname, age, name) VALUES ('Smith', '42', 'Jane');",
    ]
    seeded = _run(
        [docker, "exec", "-e", "MYSQL_PWD=barch", name] + seed, timeout=20
    )
    if seeded is None or seeded.returncode != 0:
        print("SKIP: docker mysql seed failed")
        _rm(docker, name)
        return None
    dsn = "host=127.0.0.1 port=%d user=barch password=barch database=barch" % port
    print("using docker mysql")
    box = SqlContainer(docker, name, dsn)
    atexit.register(box.stop)
    return box


def start_postgres(name="barch-foreign-postgres", port=POSTGRES_PORT):
    docker = docker_bin()
    if not docker:
        print("SKIP: docker not available")
        return None
    _rm(docker, name)
    run = _run(
        [
            docker, "run", "-d", "--name", name,
            "-e", "POSTGRES_USER=barch",
            "-e", "POSTGRES_PASSWORD=barch",
            "-e", "POSTGRES_DB=barch",
            "-p", "127.0.0.1:%d:5432" % port,
            POSTGRES_IMAGE,
        ],
        timeout=180,
    )
    if run is None or run.returncode != 0:
        print("SKIP: docker postgres failed to start")
        _rm(docker, name)
        return None
    ready = ["pg_isready", "-U", "barch", "-d", "barch"]
    if not _wait_exec(docker, name, ready):
        print("SKIP: docker postgres did not become ready")
        _rm(docker, name)
        return None
    seed = [
        "psql", "-U", "barch", "-d", "barch", "-v", "ON_ERROR_STOP=1", "-c",
        "CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT);"
        "INSERT INTO t (k, v) VALUES"
        " ('sku', 'widget'),"
        " ($$o'reilly$$, 'quoted'),"
        " ($$x'OR'1'='1$$, 'safe'),"
        " ('Smith 42', 'whole');"
        "CREATE TABLE person (surname TEXT, age TEXT, name TEXT, PRIMARY KEY (surname, age));"
        "INSERT INTO person (surname, age, name) VALUES ('Smith', '42', 'Jane');"
        "CREATE SCHEMA fp_comp;"
        "CREATE TABLE fp_comp.person (surname TEXT, age TEXT, name TEXT, PRIMARY KEY (surname, age));"
        "INSERT INTO fp_comp.person (surname, age, name) VALUES ('Smith', '42', 'Jane');",
    ]
    seeded = _run(
        [docker, "exec", "-e", "PGPASSWORD=barch", name] + seed, timeout=20
    )
    if seeded is None or seeded.returncode != 0:
        print("SKIP: docker postgres seed failed")
        _rm(docker, name)
        return None
    dsn = "host=127.0.0.1 port=%d user=barch password=barch dbname=barch" % port
    print("using docker postgres")
    box = SqlContainer(docker, name, dsn)
    atexit.register(box.stop)
    return box
