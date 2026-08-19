#include "sql.h"
#include "configuration.h"
#include "key_space.h"
#include "keyspec.h"
#include "lzr_log.h"

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#ifdef BARCH_HAS_POSTGRES
#include <libpq-fe.h>
#endif

namespace barch {
namespace foreign {

#ifdef BARCH_HAS_POSTGRES

struct pg_conn {
    PGconn* c{nullptr};
    int64_t idle_since{0};
    ~pg_conn() {
        if (c)
            PQfinish(c);
    }
};

struct postgres_pool : sql_backend {
    std::string conninfo;
    std::string space_name;
    size_t cap{8};
    uint64_t max_age_override_ms{0};
    std::mutex mu;
    std::condition_variable cv;
    std::vector<std::unique_ptr<pg_conn>> idle;
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

    std::unique_ptr<pg_conn> connect() {
        auto c = std::make_unique<pg_conn>();
        c->c = PQconnectdb(conninfo.c_str());
        if (!c->c || PQstatus(c->c) != CONNECTION_OK) {
            c.reset();
            return nullptr;
        }
        return c;
    }

    std::unique_ptr<pg_conn> checkout(uint64_t deadline_ms) {
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
                auto c = connect();
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

    void checkin(std::unique_ptr<pg_conn> c) {
        std::lock_guard lk(mu);
        if (c) {
            c->idle_since = art::now();
            idle.push_back(std::move(c));
        } else if (live > 0)
            --live;
        cv.notify_one();
    }

    result exec(PGconn* c, const compiled_query& q, const std::vector<std::string>& vals,
                uint64_t deadline_ms) {
        if (deadline_ms) {
            std::string set = "SET statement_timeout TO " + std::to_string(deadline_ms);
            PGresult* tr = PQexec(c, set.c_str());
            PQclear(tr);
        }
        std::vector<const char*> ptrs(vals.size());
        std::vector<int> lens(vals.size());
        std::vector<int> fmts(vals.size(), 0);
        for (size_t i = 0; i < vals.size(); ++i) {
            ptrs[i] = vals[i].data();
            lens[i] = static_cast<int>(vals[i].size());
        }
        PGresult* r = PQexecParams(c, q.sql.c_str(), static_cast<int>(vals.size()),
                                   nullptr, ptrs.empty() ? nullptr : ptrs.data(),
                                   lens.empty() ? nullptr : lens.data(),
                                   fmts.empty() ? nullptr : fmts.data(), 0);
        if (!r) {
            return {result::status::error, "FOREIGN postgres exec"};
        }
        auto st = PQresultStatus(r);
        if (st != PGRES_TUPLES_OK) {
            std::string e = PQresultErrorMessage(r);
            PQclear(r);
            if (e.empty())
                return {result::status::error, "FOREIGN postgres"};
            if (e.find("FOREIGN ") == 0)
                return {result::status::error, e};
            return {result::status::error, "FOREIGN " + e};
        }
        if (PQntuples(r) < 1 || PQnfields(r) < 1) {
            PQclear(r);
            return {result::status::missing, {}};
        }
        if (PQgetisnull(r, 0, 0)) {
            PQclear(r);
            return {result::status::missing, {}};
        }
        std::string v(PQgetvalue(r, 0, 0), static_cast<size_t>(PQgetlength(r, 0, 0)));
        PQclear(r);
        return {result::status::value, std::move(v)};
    }

    result query(std::string_view sql, std::string_view key, uint64_t deadline_ms) override {
        compiled_query q;
        if (!compile_query(sql, true, q, space_name))
            return {result::status::error, "FOREIGN query needs ? or $n or $$"};
        std::string err;
        std::vector<std::string> vals;
        auto ks = get_keyspace(space_name);
        if (!bind_key(key, q, vals, err, ks.get()))
            return {result::status::error, err};
        auto c = checkout(deadline_ms);
        if (!c)
            return {result::status::error, "FOREIGN timeout"};
        auto r = exec(c->c, q, vals, deadline_ms);
        if (r.status == result::status::error || PQstatus(c->c) != CONNECTION_OK)
            checkin(nullptr);
        else
            checkin(std::move(c));
        return r;
    }
};

static std::string with_connect_timeout(std::string dsn, uint64_t deadline_ms) {
    if (dsn.find("connect_timeout") != std::string::npos)
        return dsn;
    unsigned sec = deadline_ms ? static_cast<unsigned>((deadline_ms + 999) / 1000) : 1;
    if (!sec)
        sec = 1;
    return dsn + " connect_timeout=" + std::to_string(sec);
}

bool postgres_available() {
    return true;
}

bool prepare_postgres(key_space& ks) {
    std::string err;
    auto dsn = resolve_dsn(ks, err);
    if (dsn.empty()) {
        barch::err({"postgres foreign source - ignoring it for space", ks.get_name(), err});
        return false;
    }
    auto pool = std::make_shared<postgres_pool>();
    pool->cap = ks.foreign_pool_size ? ks.foreign_pool_size : 8;
    pool->max_age_override_ms = ks.foreign_pool_max_age_ms;
    pool->conninfo = with_connect_timeout(dsn, ks.foreign_query_timeout_ms);
    pool->space_name = ks.get_canonical_name();
    ks.sql = std::move(pool);
    return true;
}

struct postgres_driver_t : driver {
    result fetch(std::string_view space, std::string_view key, uint64_t deadline_ms) override {
        auto ks = get_keyspace(std::string(space));
        if (!ks || !ks->sql)
            return {result::status::error, "FOREIGN no driver"};
        if (ks->foreign_query.empty())
            return {result::status::error, "FOREIGN no query"};
        return ks->sql->query(ks->foreign_query, key, deadline_ms);
    }
};

driver& postgres_driver() {
    static postgres_driver_t d;
    return d;
}

#else

bool postgres_available() {
    return false;
}

bool prepare_postgres(key_space& ks) {
    std::string err;
    if (resolve_dsn(ks, err).empty()) {
        barch::err({"postgres foreign source - ignoring it for space", ks.get_name(), err});
        return false;
    }
    barch::err({"postgres not built - ignoring it for space", ks.get_name()});
    return false;
}

struct postgres_stub : driver {
    result fetch(std::string_view, std::string_view, uint64_t) override {
        return {result::status::error, "FOREIGN postgres not built"};
    }
};

driver& postgres_driver() {
    static postgres_stub d;
    return d;
}

#endif

}
}
