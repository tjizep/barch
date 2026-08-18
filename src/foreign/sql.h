#ifndef BARCH_FOREIGN_SQL_H
#define BARCH_FOREIGN_SQL_H

#include "driver.h"
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

bool mysql_available();
bool postgres_available();
bool prepare_mysql(key_space& ks);
bool prepare_postgres(key_space& ks);
driver& mysql_driver();
driver& postgres_driver();

}
}

#endif
