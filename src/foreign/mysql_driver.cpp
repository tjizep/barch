#include "sql.h"
#include "configuration.h"
#include "key_space.h"
#include "keyspec.h"
#include "lzr_log.h"

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#ifdef BARCH_HAS_MYSQL
#include <mysql.h>
#endif

namespace barch {
namespace foreign {

#ifdef BARCH_HAS_MYSQL

struct mysql_conn {
    MYSQL* m{nullptr};
    int64_t idle_since{0};
    ~mysql_conn() {
        if (m)
            mysql_close(m);
    }
};

static unsigned timeout_sec(uint64_t deadline_ms) {
    if (!deadline_ms)
        return 1;
    unsigned s = static_cast<unsigned>((deadline_ms + 999) / 1000);
    return s ? s : 1;
}

static bool parse_kv(std::string_view in, std::string& host, unsigned& port,
                     std::string& user, std::string& pass, std::string& db) {
    std::string cur;
    auto apply = [&](const std::string& kv) {
        auto eq = kv.find('=');
        if (eq == std::string::npos)
            return;
        std::string k = kv.substr(0, eq);
        std::string v = kv.substr(eq + 1);
        if (k == "host" || k == "hostname")
            host = v;
        else if (k == "port")
            port = static_cast<unsigned>(std::strtoul(v.c_str(), nullptr, 10));
        else if (k == "user" || k == "uid")
            user = v;
        else if (k == "password" || k == "pwd")
            pass = v;
        else if (k == "database" || k == "db" || k == "dbname")
            db = v;
    };
    for (char c : in) {
        if (c == ';' || c == ' ') {
            if (!cur.empty()) {
                apply(cur);
                cur.clear();
            }
        } else {
            cur += c;
        }
    }
    if (!cur.empty())
        apply(cur);
    return !host.empty();
}

static bool parse_uri(std::string_view in, std::string& host, unsigned& port,
                      std::string& user, std::string& pass, std::string& db) {
    auto p = in.find("://");
    if (p == std::string_view::npos)
        return false;
    auto rest = in.substr(p + 3);
    auto at = rest.find('@');
    std::string_view hp = rest;
    if (at != std::string_view::npos) {
        auto auth = rest.substr(0, at);
        auto colon = auth.find(':');
        if (colon == std::string_view::npos)
            user = std::string(auth);
        else {
            user = std::string(auth.substr(0, colon));
            pass = std::string(auth.substr(colon + 1));
        }
        hp = rest.substr(at + 1);
    }
    auto slash = hp.find('/');
    std::string_view hostport = hp;
    if (slash != std::string_view::npos) {
        hostport = hp.substr(0, slash);
        db = std::string(hp.substr(slash + 1));
        auto q = db.find('?');
        if (q != std::string::npos)
            db.resize(q);
    }
    auto colon = hostport.rfind(':');
    if (colon != std::string_view::npos) {
        host = std::string(hostport.substr(0, colon));
        port = static_cast<unsigned>(std::strtoul(std::string(hostport.substr(colon + 1)).c_str(), nullptr, 10));
    } else {
        host = std::string(hostport);
    }
    return !host.empty();
}

struct mysql_pool : sql_backend {
    std::string host{"localhost"};
    unsigned port{3306};
    std::string user;
    std::string pass;
    std::string db;
    std::string space_name;
    size_t cap{8};
    uint64_t max_age_override_ms{0};
    std::mutex mu;
    std::condition_variable cv;
    std::vector<std::unique_ptr<mysql_conn>> idle;
    size_t live{0};

    uint64_t max_age_ms() const {
        if (max_age_override_ms)
            return max_age_override_ms;
        return get_foreign_pool_max_age_ms();
    }

    void drop_idle() override {
        std::lock_guard lk(mu);
        auto n = idle.size();
        drop_idle_older_than(idle, live, max_age_ms(), art::now());
        if (idle.size() != n)
            cv.notify_all();
    }

    std::unique_ptr<mysql_conn> connect(uint64_t deadline_ms) {
        auto c = std::make_unique<mysql_conn>();
        c->m = mysql_init(nullptr);
        if (!c->m)
            return nullptr;
        unsigned sec = timeout_sec(deadline_ms);
        mysql_options(c->m, MYSQL_OPT_CONNECT_TIMEOUT, &sec);
        mysql_options(c->m, MYSQL_OPT_READ_TIMEOUT, &sec);
        mysql_options(c->m, MYSQL_OPT_WRITE_TIMEOUT, &sec);
        if (!mysql_real_connect(c->m, host.c_str(), user.empty() ? nullptr : user.c_str(),
                                pass.empty() ? nullptr : pass.c_str(),
                                db.empty() ? nullptr : db.c_str(), port, nullptr, 0)) {
            c.reset();
            return nullptr;
        }
        return c;
    }

    std::unique_ptr<mysql_conn> checkout(uint64_t deadline_ms) {
        int64_t until = art::now() + static_cast<int64_t>(deadline_ms ? deadline_ms : 1000);
        std::unique_lock lk(mu);
        for (;;) {
            if (!idle.empty()) {
                auto c = std::move(idle.back());
                idle.pop_back();
                return c;
            }
            if (live < cap) {
                ++live;
                lk.unlock();
                auto c = connect(deadline_ms);
                if (!c) {
                    std::lock_guard g(mu);
                    --live;
                    cv.notify_one();
                }
                return c;
            }
            auto left = until - art::now();
            if (left <= 0)
                return nullptr;
            cv.wait_for(lk, std::chrono::milliseconds(left));
        }
    }

    void checkin(std::unique_ptr<mysql_conn> c) {
        std::lock_guard lk(mu);
        if (c) {
            c->idle_since = art::now();
            idle.push_back(std::move(c));
        } else if (live > 0)
            --live;
        cv.notify_one();
    }

    result exec(MYSQL* m, const compiled_query& q, std::vector<std::string>& vals) {
        MYSQL_STMT* st = mysql_stmt_init(m);
        if (!st)
            return {result::status::error, "FOREIGN mysql stmt"};
        if (mysql_stmt_prepare(st, q.sql.data(), q.sql.size()) != 0) {
            std::string e = mysql_stmt_error(st);
            mysql_stmt_close(st);
            return {result::status::error, e.empty() ? "FOREIGN mysql prepare" : "FOREIGN " + e};
        }
        std::vector<MYSQL_BIND> in(vals.size());
        std::vector<unsigned long> lens(vals.size());
        for (size_t i = 0; i < vals.size(); ++i) {
            lens[i] = static_cast<unsigned long>(vals[i].size());
            in[i].buffer_type = MYSQL_TYPE_STRING;
            in[i].buffer = const_cast<char*>(vals[i].data());
            in[i].buffer_length = lens[i];
            in[i].length = &lens[i];
        }
        if (!in.empty() && mysql_stmt_bind_param(st, in.data()) != 0) {
            mysql_stmt_close(st);
            return {result::status::error, "FOREIGN mysql bind"};
        }
        if (mysql_stmt_execute(st) != 0) {
            std::string e = mysql_stmt_error(st);
            mysql_stmt_close(st);
            return {result::status::error, e.empty() ? "FOREIGN mysql execute" : "FOREIGN " + e};
        }
        char buf[65536];
        unsigned long vlen = 0;
        MYSQL_BIND out{};
        out.buffer_type = MYSQL_TYPE_STRING;
        out.buffer = buf;
        out.buffer_length = sizeof(buf) - 1;
        out.length = &vlen;
        bool isnull = false;
        out.is_null = &isnull;
        if (mysql_stmt_bind_result(st, &out) != 0) {
            mysql_stmt_close(st);
            return {result::status::error, "FOREIGN mysql bind result"};
        }
        mysql_stmt_store_result(st);
        int fr = mysql_stmt_fetch(st);
        mysql_stmt_close(st);
        if (fr == MYSQL_NO_DATA)
            return {result::status::missing, {}};
        if (fr != 0)
            return {result::status::error, "FOREIGN mysql fetch"};
        if (isnull)
            return {result::status::missing, {}};
        return {result::status::value, std::string(buf, vlen)};
    }

    result query(std::string_view sql, std::string_view key, uint64_t deadline_ms) override {
        compiled_query q;
        if (!compile_query(sql, false, q, space_name))
            return {result::status::error, "FOREIGN query needs ? or $n or $$"};
        std::string err;
        std::vector<std::string> vals;
        auto ks = get_keyspace(space_name);
        if (!bind_key(key, q, vals, err, ks.get()))
            return {result::status::error, err};
        auto c = checkout(deadline_ms);
        if (!c)
            return {result::status::error, "FOREIGN timeout"};
        auto r = exec(c->m, q, vals);
        if (r.status == result::status::error)
            checkin(nullptr);
        else
            checkin(std::move(c));
        return r;
    }
};

static bool parse_mysql_dsn(const std::string& dsn, mysql_pool& p) {
    if (dsn.find("://") != std::string::npos)
        return parse_uri(dsn, p.host, p.port, p.user, p.pass, p.db);
    return parse_kv(dsn, p.host, p.port, p.user, p.pass, p.db);
}

bool mysql_available() {
    return true;
}

bool prepare_mysql(key_space& ks) {
    std::string err;
    auto dsn = resolve_dsn(ks, err);
    if (dsn.empty()) {
        barch::err({"mysql foreign source - ignoring it for space", ks.get_name(), err});
        return false;
    }
    auto pool = std::make_shared<mysql_pool>();
    pool->cap = ks.foreign_pool_size ? ks.foreign_pool_size : 8;
    pool->max_age_override_ms = ks.foreign_pool_max_age_ms;
    if (!parse_mysql_dsn(dsn, *pool) && ks.foreign_host.empty()) {
        barch::err({"mysql foreign source - ignoring it for space", ks.get_name(), "bad dsn"});
        return false;
    }
    if (pool->host.empty())
        pool->host = ks.foreign_host.empty() ? "localhost" : ks.foreign_host;
    if (ks.foreign_port)
        pool->port = static_cast<unsigned>(ks.foreign_port);
    if (pool->user.empty())
        pool->user = ks.foreign_user;
    if (pool->pass.empty())
        pool->pass = ks.foreign_password;
    if (pool->db.empty())
        pool->db = ks.foreign_database;
    pool->space_name = ks.get_canonical_name();
    ks.sql = std::move(pool);
    return true;
}

struct mysql_driver_t : driver {
    result fetch(std::string_view space, std::string_view key, uint64_t deadline_ms) override {
        auto ks = get_keyspace(std::string(space));
        if (!ks || !ks->sql)
            return {result::status::error, "FOREIGN no driver"};
        if (ks->foreign_query.empty())
            return {result::status::error, "FOREIGN no query"};
        return ks->sql->query(ks->foreign_query, key, deadline_ms);
    }
};

driver& mysql_driver() {
    static mysql_driver_t d;
    return d;
}

#else

bool mysql_available() {
    return false;
}

bool prepare_mysql(key_space& ks) {
    std::string err;
    if (resolve_dsn(ks, err).empty()) {
        barch::err({"mysql foreign source - ignoring it for space", ks.get_name(), err});
        return false;
    }
    barch::err({"mysql not built - ignoring it for space", ks.get_name()});
    return false;
}

struct mysql_stub : driver {
    result fetch(std::string_view, std::string_view, uint64_t) override {
        return {result::status::error, "FOREIGN mysql not built"};
    }
};

driver& mysql_driver() {
    static mysql_stub d;
    return d;
}

#endif

}
}
