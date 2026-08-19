#ifndef BARCH_FOREIGN_SQL_H
#define BARCH_FOREIGN_SQL_H

#include "driver.h"
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace barch {
class key_space;
namespace foreign {

struct sql_backend {
    virtual ~sql_backend() = default;
    virtual result query(std::string_view sql, std::string_view key, uint64_t deadline_ms) = 0;
    /** close idle connections older than the pool max age. default is nothing. */
    virtual void drop_idle() {}
};

enum class bind_kind { user, encoded, part };

struct query_bind {
    bind_kind kind{bind_kind::user};
    unsigned index{0};
};

struct compiled_query {
    std::string sql{};
    std::vector<query_bind> binds{};
};

std::string user_key(std::string_view internal);
std::vector<std::string> key_parts(std::string_view internal);
/** `$n` parts. Uses `<name>.key_split` when set, otherwise spaces / the composite. */
std::vector<std::string> key_parts(std::string_view internal, const key_space* ks);
std::string key_encoded(std::string_view internal);
/** file:/path, env:VAR, the raw DSN, or host=… from the space fields. Never log the return. */
std::string resolve_dsn(const key_space& ks, std::string& err);
/** `?` is the whole user key, `$n` is component n, `$$` is encoded_key_as_string.
 *  `$k` is the keyspace name, written into the SQL as an identifier.
 *  `\$` is a literal `$`. */
bool compile_query(std::string_view sql, bool postgres, compiled_query& out,
                   std::string_view space = {});
bool query_has_placeholder(std::string_view sql);
bool query_has_one_placeholder(std::string_view sql);
bool bind_key(std::string_view internal, const compiled_query& q,
              std::vector<std::string>& values, std::string& err,
              const key_space* ks = nullptr);
std::string postgres_placeholders(std::string_view sql);

/** drop idle MySQL/PG connections older than max_age_ms. 0 means keep them. */
template<typename Conn>
void drop_idle_older_than(std::vector<std::unique_ptr<Conn>>& idle, size_t& live,
                          uint64_t max_age_ms, int64_t now) {
    if (!max_age_ms)
        return;
    size_t keep = 0;
    for (size_t i = 0; i < idle.size(); ++i) {
        if (now - idle[i]->idle_since >= static_cast<int64_t>(max_age_ms)) {
            if (live > 0)
                --live;
        } else {
            if (keep != i)
                idle[keep] = std::move(idle[i]);
            ++keep;
        }
    }
    idle.resize(keep);
}

bool mysql_available();
bool postgres_available();
bool prepare_mysql(key_space& ks);
bool prepare_postgres(key_space& ks);
driver& mysql_driver();
driver& postgres_driver();

}
}

#endif
