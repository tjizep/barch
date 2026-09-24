#include "mail_luau.h"

#ifdef BARCH_HAS_COFETCH

#include "driver.h"
#include "lzr_log.h"
#include "swig_api.h"

#include <curl/curl.h>

#include <condition_variable>
#include <cstring>
#include <ctime>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "lua.h"
#include "lualib.h"

/*
 * mail.send{...} - SMTP from a stored function, TODO 368.
 *
 * Built on libcurl's SMTP rather than cofetch, which only speaks HTTP. A send
 * runs as a blocking curl_easy_perform on a small pool of mail threads, and the
 * calling coroutine is parked meanwhile, so it costs a mail thread and not a
 * function pool thread. SMTP is a short conversation and mail is meant to go
 * through a queue consumer anyway, so a handful of threads is plenty.
 *
 * Where the server is and how to log in to it comes from the configuration
 * space, read fresh on every send so a change takes effect without a restart.
 * The password is never visible to a script: `configuration` hides every key
 * ending in "password" from barch.space reads - see hide_secrets in
 * function_api.cpp.
 */

namespace {

constexpr long default_timeout_ms = 30000;
constexpr size_t mail_threads = 4;

/** what the configuration space says about the SMTP server */
struct mail_settings {
    std::string server;     // smtp://host:port or smtps://host:port
    std::string user;
    std::string password;
    std::string from;       // used when the script names no sender
    std::string starttls;   // try (default), require or off; smtp:// only
    bool verify{true};      // check the server's certificate
    long timeout_ms{default_timeout_ms};
};

/** one message, ready to go - everything the mail thread needs, nothing from Lua */
struct mail_job {
    mail_settings settings;
    std::string envelope_from;
    std::vector<std::string> recipients;
    std::string message;
    std::string message_id;
    long timeout_ms{default_timeout_ms};
};

struct mail_result {
    bool ok{false};
    long code{0};           // the last SMTP reply code, 250 on success
    std::string error;
    std::string message_id;
};

// --- the mail threads --------------------------------------------------------

/*
 * Process lifetime and never destroyed, like the http reactor: the threads are
 * parked on the condition variable and there is nothing useful to do with a
 * half sent message at exit.
 */
struct mail_pool {
    std::mutex mu;
    std::condition_variable cv;
    std::deque<std::function<void()>> jobs;

    mail_pool() {
        curl_global_init(CURL_GLOBAL_DEFAULT);
        for (size_t i = 0; i < mail_threads; ++i) {
            std::thread([this] { run(); }).detach();
        }
    }

    void run() {
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lk(mu);
                cv.wait(lk, [this] { return !jobs.empty(); });
                job = std::move(jobs.front());
                jobs.pop_front();
            }
            try {
                job();
            } catch (const std::exception& e) {
                barch::err({"mail thread", e.what()});
            }
        }
    }

    void post(std::function<void()> job) {
        {
            std::lock_guard<std::mutex> lk(mu);
            jobs.push_back(std::move(job));
        }
        cv.notify_one();
    }
};

mail_pool& pool() {
    static mail_pool* p = new mail_pool();
    return *p;
}

struct upload {
    const std::string* data;
    size_t at{0};
};

size_t read_message(char* buf, size_t size, size_t n, void* user) {
    auto* up = static_cast<upload*>(user);
    size_t room = size * n;
    size_t left = up->data->size() - up->at;
    size_t take = left < room ? left : room;
    memcpy(buf, up->data->data() + up->at, take);
    up->at += take;
    return take;
}

mail_result deliver(const mail_job& job) {
    mail_result out;
    out.message_id = job.message_id;
    CURL* c = curl_easy_init();
    if (!c) {
        out.error = "could not start a mail session";
        return out;
    }
    char errbuf[CURL_ERROR_SIZE] = {0};
    curl_slist* rcpt = nullptr;
    for (const auto& r : job.recipients)
        rcpt = curl_slist_append(rcpt, ("<" + r + ">").c_str());
    upload up{&job.message, 0};
    const auto& s = job.settings;

    curl_easy_setopt(c, CURLOPT_URL, s.server.c_str());
    // several threads, so no SIGALRM for the resolver timeout
    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(c, CURLOPT_ERRORBUFFER, errbuf);
    curl_easy_setopt(c, CURLOPT_MAIL_FROM, ("<" + job.envelope_from + ">").c_str());
    curl_easy_setopt(c, CURLOPT_MAIL_RCPT, rcpt);
    curl_easy_setopt(c, CURLOPT_READFUNCTION, read_message);
    curl_easy_setopt(c, CURLOPT_READDATA, &up);
    curl_easy_setopt(c, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(c, CURLOPT_TIMEOUT_MS, job.timeout_ms);
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT_MS, job.timeout_ms);
    if (!s.user.empty()) {
        curl_easy_setopt(c, CURLOPT_USERNAME, s.user.c_str());
        curl_easy_setopt(c, CURLOPT_PASSWORD, s.password.c_str());
    }
    // smtps:// is TLS from the first byte and ignores this. On smtp:// it decides
    // whether STARTTLS is tried, insisted on, or left alone
    long use_ssl = CURLUSESSL_TRY;
    if (s.starttls == "require")
        use_ssl = CURLUSESSL_ALL;
    else if (s.starttls == "off")
        use_ssl = CURLUSESSL_NONE;
    curl_easy_setopt(c, CURLOPT_USE_SSL, use_ssl);
    if (!s.verify) {
        curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(c, CURLOPT_SSL_VERIFYHOST, 0L);
    }

    CURLcode rc = curl_easy_perform(c);
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &out.code);
    if (rc == CURLE_OK) {
        out.ok = true;
    } else {
        out.error = errbuf[0] ? errbuf : curl_easy_strerror(rc);
        // curl leaves a trailing newline on some of its messages
        while (!out.error.empty() && (out.error.back() == '\n' || out.error.back() == '\r'))
            out.error.pop_back();
    }
    curl_slist_free_all(rcpt);
    curl_easy_cleanup(c);
    return out;
}

// --- building the message ----------------------------------------------------

const char b64chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string base64(const std::string& in) {
    std::string out;
    out.reserve((in.size() + 2) / 3 * 4);
    size_t i = 0;
    while (i + 2 < in.size()) {
        uint32_t v = ((uint8_t) in[i] << 16) | ((uint8_t) in[i + 1] << 8) | (uint8_t) in[i + 2];
        out += b64chars[(v >> 18) & 63];
        out += b64chars[(v >> 12) & 63];
        out += b64chars[(v >> 6) & 63];
        out += b64chars[v & 63];
        i += 3;
    }
    if (i + 1 == in.size()) {
        uint32_t v = (uint8_t) in[i] << 16;
        out += b64chars[(v >> 18) & 63];
        out += b64chars[(v >> 12) & 63];
        out += "==";
    } else if (i + 2 == in.size()) {
        uint32_t v = ((uint8_t) in[i] << 16) | ((uint8_t) in[i + 1] << 8);
        out += b64chars[(v >> 18) & 63];
        out += b64chars[(v >> 12) & 63];
        out += b64chars[(v >> 6) & 63];
        out += '=';
    }
    return out;
}

/** a body as base64 in 76 character lines, which is what MIME allows */
std::string base64_body(const std::string& text) {
    // text parts are meant to be CRLF before they are encoded
    std::string crlf;
    crlf.reserve(text.size() + text.size() / 32);
    for (size_t i = 0; i < text.size(); ++i) {
        if (text[i] == '\n' && (i == 0 || text[i - 1] != '\r'))
            crlf += '\r';
        crlf += text[i];
    }
    std::string enc = base64(crlf);
    std::string out;
    for (size_t i = 0; i < enc.size(); i += 76) {
        out.append(enc, i, 76);
        out += "\r\n";
    }
    return out;
}

bool is_ascii(const std::string& s) {
    for (unsigned char ch : s) {
        if (ch >= 0x80)
            return false;
    }
    return true;
}

/*
 * RFC 2047 for a header value that is not plain ASCII - a subject, a display
 * name. Cut into words of at most 45 bytes, which keeps each encoded word under
 * the 75 characters the RFC allows, and never inside a UTF-8 sequence.
 */
std::string encode_word(const std::string& s) {
    if (is_ascii(s))
        return s;
    std::string out;
    size_t i = 0;
    while (i < s.size()) {
        size_t end = i + 45 < s.size() ? i + 45 : s.size();
        while (end < s.size() && end > i && ((unsigned char) s[end] & 0xC0) == 0x80)
            --end;
        if (!out.empty())
            out += "\r\n ";
        out += "=?UTF-8?B?" + base64(s.substr(i, end - i)) + "?=";
        i = end;
    }
    return out;
}

bool has_line_break(const std::string& s) {
    return s.find_first_of("\r\n") != std::string::npos;
}

/*
 * One address as a script wrote it - "a@example.com" or "Name <a@example.com>".
 * `bare` is what goes on the envelope, `header` what goes in To/From.
 */
bool parse_address(const std::string& in, std::string& bare, std::string& header,
                   std::string& err) {
    if (has_line_break(in)) {
        err = "an address cannot contain a line break";
        return false;
    }
    std::string name;
    auto lt = in.rfind('<');
    if (lt != std::string::npos) {
        auto gt = in.find('>', lt);
        if (gt == std::string::npos || gt + 1 != in.find_last_not_of(" \t") + 1) {
            err = "'" + in + "' is not an address";
            return false;
        }
        bare = in.substr(lt + 1, gt - lt - 1);
        name = in.substr(0, lt);
        while (!name.empty() && (name.back() == ' ' || name.back() == '\t'))
            name.pop_back();
        size_t a = name.find_first_not_of(" \t");
        name = a == std::string::npos ? "" : name.substr(a);
    } else {
        size_t a = in.find_first_not_of(" \t");
        size_t b = in.find_last_not_of(" \t");
        bare = a == std::string::npos ? "" : in.substr(a, b - a + 1);
    }
    auto at = bare.find('@');
    if (bare.empty() || at == std::string::npos || at == 0 || at + 1 == bare.size()
        || bare.find_first_of(" \t<>,;\"") != std::string::npos || !is_ascii(bare)) {
        err = "'" + in + "' is not an address";
        return false;
    }
    if (name.empty())
        header = bare;
    else if (is_ascii(name))
        header = name + " <" + bare + ">";
    else
        header = encode_word(name) + " <" + bare + ">";
    return true;
}

std::string rfc5322_date() {
    static const char* days[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
    static const char* months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    time_t now = time(nullptr);
    struct tm t{};
    gmtime_r(&now, &t);
    char buf[64];
    snprintf(buf, sizeof buf, "%s, %02d %s %04d %02d:%02d:%02d +0000", days[t.tm_wday],
             t.tm_mday, months[t.tm_mon], t.tm_year + 1900, t.tm_hour, t.tm_min, t.tm_sec);
    return buf;
}

std::string random_hex(size_t bytes) {
    thread_local std::mt19937_64 gen{std::random_device{}()};
    static const char hex[] = "0123456789abcdef";
    std::string out;
    for (size_t i = 0; i < bytes; ++i) {
        auto b = (unsigned) (gen() & 0xff);
        out += hex[b >> 4];
        out += hex[b & 15];
    }
    return out;
}

// --- reading the call --------------------------------------------------------

/** a field that may be one address or a list of them */
bool read_addresses(lua_State* L, int t, const char* field, std::vector<std::string>& out,
                    std::string& err) {
    lua_getfield(L, t, field);
    bool ok = true;
    if (lua_isstring(L, -1)) {
        out.emplace_back(lua_tostring(L, -1));
    } else if (lua_istable(L, -1)) {
        int n = lua_objlen(L, -1);
        for (int i = 1; i <= n; ++i) {
            lua_rawgeti(L, -1, i);
            if (!lua_isstring(L, -1)) {
                err = std::string("mail.send ") + field + " must be strings";
                ok = false;
            } else {
                out.emplace_back(lua_tostring(L, -1));
            }
            lua_pop(L, 1);
            if (!ok)
                break;
        }
    } else if (!lua_isnil(L, -1)) {
        err = std::string("mail.send ") + field + " must be an address or a list of them";
        ok = false;
    }
    lua_pop(L, 1);
    return ok;
}

bool read_string(lua_State* L, int t, const char* field, std::string& out, std::string& err) {
    lua_getfield(L, t, field);
    bool ok = true;
    if (lua_isstring(L, -1)) {
        size_t n = 0;
        const char* s = lua_tolstring(L, -1, &n);
        out.assign(s, n);
    } else if (!lua_isnil(L, -1)) {
        err = std::string("mail.send ") + field + " must be a string";
        ok = false;
    }
    lua_pop(L, 1);
    return ok;
}

mail_settings read_settings() {
    mail_settings s;
    KeyValue conf("configuration");
    s.server = conf.get("mail.server");
    s.user = conf.get("mail.user");
    s.password = conf.get("mail.password");
    s.from = conf.get("mail.from");
    s.starttls = conf.get("mail.starttls");
    auto verify = conf.get("mail.verify");
    s.verify = !(verify == "0" || verify == "off" || verify == "no" || verify == "false");
    auto t = conf.get("mail.timeout_ms");
    if (!t.empty()) {
        char* end = nullptr;
        long v = strtol(t.c_str(), &end, 10);
        if (end && *end == 0 && v > 0)
            s.timeout_ms = v;
    }
    return s;
}

/*
 * Turn the script's table into a message. Anything wrong with what the script
 * asked for comes back as an error here and is raised: it is a bug in the
 * script, and no amount of retrying will send it.
 */
bool build_job(lua_State* L, mail_job& job, std::string& err) {
    job.settings = read_settings();
    const auto& s = job.settings;
    if (s.server.empty()) {
        err = "mail.send needs mail.server in the configuration space";
        return false;
    }
    if (s.server.rfind("smtp://", 0) != 0 && s.server.rfind("smtps://", 0) != 0) {
        err = "mail.server must start with smtp:// or smtps://";
        return false;
    }
    if (!s.starttls.empty() && s.starttls != "try" && s.starttls != "require"
        && s.starttls != "off") {
        err = "mail.starttls is try, require or off";
        return false;
    }

    std::vector<std::string> to, cc, bcc;
    std::string from, reply_to, subject, text, html, message_id;
    if (!read_addresses(L, 1, "to", to, err) || !read_addresses(L, 1, "cc", cc, err)
        || !read_addresses(L, 1, "bcc", bcc, err) || !read_string(L, 1, "from", from, err)
        || !read_string(L, 1, "reply_to", reply_to, err)
        || !read_string(L, 1, "subject", subject, err) || !read_string(L, 1, "text", text, err)
        || !read_string(L, 1, "html", html, err)
        || !read_string(L, 1, "message_id", message_id, err))
        return false;

    lua_getfield(L, 1, "timeout");
    if (!lua_isnil(L, -1)) {
        long ms = (long) lua_tointeger(L, -1);
        if (ms <= 0) {
            lua_pop(L, 1);
            err = "mail.send timeout must be a positive number of milliseconds";
            return false;
        }
        job.timeout_ms = ms;
    } else {
        job.timeout_ms = s.timeout_ms;
    }
    lua_pop(L, 1);

    if (to.empty() && cc.empty() && bcc.empty()) {
        err = "mail.send needs someone to send to";
        return false;
    }
    if (text.empty() && html.empty()) {
        err = "mail.send needs text or html";
        return false;
    }
    if (from.empty())
        from = s.from;
    if (from.empty()) {
        err = "mail.send needs a from, or mail.from in the configuration space";
        return false;
    }
    if (has_line_break(subject)) {
        err = "mail.send subject cannot contain a line break";
        return false;
    }

    std::string bare, header_from;
    if (!parse_address(from, bare, header_from, err))
        return false;
    job.envelope_from = bare;
    std::string domain = bare.substr(bare.find('@') + 1);

    auto header_list = [&](const std::vector<std::string>& list, bool envelope,
                           std::string& line) -> bool {
        for (const auto& a : list) {
            std::string b, h;
            if (!parse_address(a, b, h, err))
                return false;
            if (envelope)
                job.recipients.push_back(b);
            if (!line.empty())
                line += ",\r\n ";
            line += h;
        }
        return true;
    };
    std::string to_line, cc_line, bcc_ignored, reply_line;
    if (!header_list(to, true, to_line) || !header_list(cc, true, cc_line)
        || !header_list(bcc, true, bcc_ignored))
        return false;
    if (!reply_to.empty()) {
        std::string b;
        if (!parse_address(reply_to, b, reply_line, err))
            return false;
    }

    if (message_id.empty()) {
        message_id = "<" + random_hex(16) + "@" + domain + ">";
    } else {
        // a queue consumer passes its own so a retried send carries the same id
        // and the receiving end can tell it is a repeat
        if (message_id.front() != '<')
            message_id = "<" + message_id + ">";
        if (has_line_break(message_id) || message_id.find('@') == std::string::npos
            || message_id.back() != '>' || !is_ascii(message_id)) {
            err = "mail.send message_id must look like <something@domain>";
            return false;
        }
    }
    job.message_id = message_id;

    std::string m;
    m += "Date: " + rfc5322_date() + "\r\n";
    m += "From: " + header_from + "\r\n";
    // a message with only Bcc recipients still wants a To, or some servers add
    // one of their own naming everyone
    m += "To: " + (to_line.empty() ? std::string("undisclosed-recipients:;") : to_line) + "\r\n";
    if (!cc_line.empty())
        m += "Cc: " + cc_line + "\r\n";
    if (!reply_line.empty())
        m += "Reply-To: " + reply_line + "\r\n";
    m += "Subject: " + encode_word(subject) + "\r\n";
    m += "Message-ID: " + message_id + "\r\n";
    m += "MIME-Version: 1.0\r\n";
    if (!text.empty() && !html.empty()) {
        std::string boundary = "=_barch_" + random_hex(12);
        m += "Content-Type: multipart/alternative; boundary=\"" + boundary + "\"\r\n\r\n";
        m += "--" + boundary + "\r\n";
        m += "Content-Type: text/plain; charset=utf-8\r\n";
        m += "Content-Transfer-Encoding: base64\r\n\r\n";
        m += base64_body(text);
        m += "--" + boundary + "\r\n";
        m += "Content-Type: text/html; charset=utf-8\r\n";
        m += "Content-Transfer-Encoding: base64\r\n\r\n";
        m += base64_body(html);
        m += "--" + boundary + "--\r\n";
    } else {
        m += std::string("Content-Type: ") + (html.empty() ? "text/plain" : "text/html")
             + "; charset=utf-8\r\n";
        m += "Content-Transfer-Encoding: base64\r\n\r\n";
        m += base64_body(html.empty() ? text : html);
    }
    job.message = std::move(m);
    return true;
}

int push_result(lua_State* L, const mail_result& r) {
    lua_newtable(L);
    lua_pushboolean(L, r.ok);
    lua_setfield(L, -2, "ok");
    lua_pushinteger(L, (int) r.code);
    lua_setfield(L, -2, "code");
    lua_pushlstring(L, r.message_id.data(), r.message_id.size());
    lua_setfield(L, -2, "message_id");
    if (!r.error.empty()) {
        lua_pushlstring(L, r.error.data(), r.error.size());
        lua_setfield(L, -2, "error");
    }
    return 1;
}

struct sync_box {
    std::mutex mu;
    std::condition_variable cv;
    bool done{false};
    mail_result result;
};

int mail_send(lua_State* L) {
    const auto* acc = barch::foreign::current_access(L);
    if (acc && !acc->may_reach_out)
        luaL_error(L, "FUNCTION mail.send needs the outbound category");
    // the settings come out of the configuration space, which takes shard latches
    if (barch::foreign::in_locked_region(L))
        luaL_error(L, "FUNCTION mail.send is not allowed inside a locked region");
    luaL_checktype(L, 1, LUA_TTABLE);

    auto job = std::make_shared<mail_job>();
    std::string err;
    if (!build_job(L, *job, err))
        luaL_error(L, "%s", err.c_str());

    if (auto parked = barch::foreign::park_call(L)) {
        pool().post([job, parked] {
            mail_result r = deliver(*job);
            barch::foreign::complete_call(parked, [r](lua_State* T) {
                return push_result(T, r);
            });
        });
        return lua_yield(L, 0);
    }

    // a Crow handler cannot yield - see fetch_verb in fetch_luau.cpp - so it waits
    // off the deadline and cut short at the wall ceiling - TODO 435
    if (uint64_t cap = barch::foreign::blocking_wait_cap(L); cap && cap < (uint64_t) job->timeout_ms)
        job->timeout_ms = (long) cap;
    const int64_t started = barch::foreign::blocking_wait_start(L);
    auto box = std::make_shared<sync_box>();
    pool().post([job, box] {
        mail_result r = deliver(*job);
        {
            std::lock_guard<std::mutex> lk(box->mu);
            box->result = std::move(r);
            box->done = true;
        }
        box->cv.notify_one();
    });
    std::unique_lock<std::mutex> lk(box->mu);
    box->cv.wait(lk, [&] { return box->done; });
    mail_result got = std::move(box->result);
    lk.unlock();
    barch::foreign::blocking_wait_end(L, started);
    return push_result(L, got);
}

} // namespace

void luaopen_mail(lua_State* L) {
    lua_newtable(L);
    lua_pushcfunction(L, mail_send, "send");
    lua_setfield(L, -2, "send");
    lua_setglobal(L, "mail");
}

#else

void luaopen_mail(lua_State*) {}

#endif
