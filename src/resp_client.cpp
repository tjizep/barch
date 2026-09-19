#include "resp_client.h"

#include <array>
#include <chrono>
#include <deque>
#include <future>
#include <memory>
#include <thread>
#include <unordered_map>

#include <sys/socket.h>
#include <cerrno>

#include <asio.hpp>

#include "barch_apis.h"
#include "caller.h"
#include "lzr_log.h"

namespace barch::resp_client {

namespace {

using asio::ip::tcp;
using clock_t_ = std::chrono::steady_clock;

struct connection {
    connection(asio::io_context& io, const resp::limits& l) : sock(io), parser(l) {}
    tcp::socket sock;
    resp::reply_parser parser;
    clock_t_::time_point idle_since{};
};
typedef std::shared_ptr<connection> conn_ptr;

struct pool_t {
    std::string label;          // for RESP POOL: host, port, user, db, protocol - no password
    std::deque<conn_ptr> idle;  // oldest at the front
    size_t open{0};             // idle and in use together
    size_t busy{0};
};

struct exchange;

/*
 * The client's own thread and everything only it touches. Process lifetime and never
 * destroyed, like the http.request reactor: the thread is parked in io.run(), and
 * there is nothing useful to do with a half finished exchange at exit.
 */
struct reactor {
    asio::io_context io;
    asio::executor_work_guard<asio::io_context::executor_type> guard;
    std::thread thread;
    asio::steady_timer reaper;
    bool reaper_armed{false};

    std::unordered_map<std::string, pool_t> pools;
    size_t total_open{0};
    // the latest the configuration said; every run carries what it read
    size_t max_connections{256};
    size_t max_idle{8};
    uint64_t idle_ms{60000};

    uint64_t connects{0}, reuses{0}, discarded{0}, timeouts{0}, refusals{0}, failures{0};

    reactor() : guard(asio::make_work_guard(io)), reaper(io) {
        thread = std::thread([this] {
            for (;;) {
                try {
                    io.run();
                    return;
                } catch (const std::exception& e) {
                    // a handler threw; log it and keep the thread, or every parked
                    // call waiting on it would wait for ever
                    barch::err({"resp client reactor", e.what()});
                }
            }
        });
        thread.detach();
    }
};

reactor& the_reactor() {
    static reactor* r = new reactor();
    return *r;
}

std::string upper(const std::string& s) {
    std::string u = s;
    for (auto& ch : u)
        ch = (char) toupper((unsigned char) ch);
    return u;
}

/** whether running these leaves the connection in a state the next user mustn't get */
bool changes_session(const std::vector<command>& cmds) {
    int multi = 0;
    for (const auto& c : cmds) {
        if (c.empty())
            continue;
        auto w = upper(c[0]);
        if (w == "MULTI")
            ++multi;
        else if (w == "EXEC" || w == "DISCARD")
            multi = multi > 0 ? multi - 1 : 0;
        else if (w == "SELECT" || w == "AUTH" || w == "HELLO" || w == "CLIENT" || w == "WATCH"
                 || w == "UNWATCH" || w == "RESET" || w == "READONLY" || w == "READWRITE"
                 || w == "USE")   // barch's key space switch is session state too
            return true;
    }
    return multi != 0;              // a MULTI left open
}

std::string encode(const std::vector<command>& cmds) {
    std::string w;
    for (const auto& c : cmds) {
        w += "*" + std::to_string(c.size()) + "\r\n";
        for (const auto& a : c) {
            w += "$" + std::to_string(a.size()) + "\r\n";
            w += a;
            w += "\r\n";
        }
    }
    return w;
}

std::string key_of(const endpoint& ep) {
    // the password is in the key only as a fingerprint, so two scripts with different
    // credentials for the same server never share a connection
    return ep.host + "\n" + std::to_string(ep.port) + "\n" + ep.user + "\n"
           + std::to_string(ep.db) + "\n" + std::to_string(ep.protocol) + "\n"
           + std::to_string(std::hash<std::string>{}(ep.password));
}

std::string label_of(const endpoint& ep) {
    std::string l = ep.host + ":" + std::to_string(ep.port);
    if (!ep.user.empty())
        l += " user=" + ep.user;
    if (ep.db >= 0)
        l += " db=" + std::to_string(ep.db);
    l += " resp=" + std::to_string(ep.protocol);
    return l;
}

/**
 * Whether an idle connection is still worth using. The server may have closed it
 * while it sat in the pool; a zero byte peek says so without taking anything off the
 * stream. Bytes waiting on an idle connection are a reply nobody asked for, which is
 * as bad.
 */
bool still_good(const conn_ptr& c) {
    if (!c->sock.is_open() || !c->parser.idle())
        return false;
    char b;
    ssize_t n = ::recv(c->sock.native_handle(), &b, 1, MSG_PEEK | MSG_DONTWAIT);
    if (n == 0)
        return false;                       // closed by the server
    if (n > 0)
        return false;                       // something unasked for is waiting
    return errno == EAGAIN || errno == EWOULDBLOCK;
}

void close_conn(reactor& r, pool_t& p, const conn_ptr& c) {
    asio::error_code ignored;
    c->sock.close(ignored);
    if (p.open > 0)
        --p.open;
    if (r.total_open > 0)
        --r.total_open;
}

void arm_reaper(reactor& r);

void reap(reactor& r) {
    r.reaper_armed = false;
    auto now = clock_t_::now();
    auto limit = std::chrono::milliseconds(r.idle_ms);
    for (auto it = r.pools.begin(); it != r.pools.end();) {
        auto& p = it->second;
        while (!p.idle.empty() && now - p.idle.front()->idle_since >= limit) {
            close_conn(r, p, p.idle.front());
            p.idle.pop_front();
        }
        // a pool with nothing open is forgotten, so the map doesn't grow with every
        // endpoint a script ever named
        if (p.open == 0 && p.busy == 0)
            it = r.pools.erase(it);
        else
            ++it;
    }
    arm_reaper(r);
}

void arm_reaper(reactor& r) {
    if (r.reaper_armed)
        return;
    bool any_idle = false;
    for (const auto& kv : r.pools) {
        if (!kv.second.idle.empty()) {
            any_idle = true;
            break;
        }
    }
    if (!any_idle)
        return;
    r.reaper_armed = true;
    auto every = std::chrono::milliseconds(std::min<uint64_t>(std::max<uint64_t>(r.idle_ms / 4, 50), 1000));
    r.reaper.expires_after(every);
    r.reaper.async_wait([&r](const asio::error_code& ec) {
        if (!ec)
            reap(r);
        else
            r.reaper_armed = false;
    });
}

/*
 * One run of commands on one connection. Everything it does happens on the reactor
 * thread; every handler holds the exchange alive with a shared_ptr and checks
 * `finished` first, so whatever arrives after the end - a cancelled read, a timer
 * that fired late - does nothing.
 */
struct exchange : std::enable_shared_from_this<exchange> {
    exchange(reactor& r, endpoint e, settings s, std::vector<command> c, done_fn d)
        : r(r), ep(std::move(e)), set(s), cmds(std::move(c)), done(std::move(d)),
          timer(r.io), resolver(r.io) {}

    reactor& r;
    endpoint ep;
    settings set;
    std::vector<command> cmds;
    done_fn done;
    std::string key;
    conn_ptr conn;
    asio::steady_timer timer;
    tcp::resolver resolver;
    std::string wire;
    std::vector<Variable> got;
    size_t expect{0};
    bool in_handshake{false};
    bool finished{false};
    std::array<char, 64 * 1024> rbuf{};

    void start() {
        r.max_connections = set.max_connections;
        r.max_idle = set.max_idle;
        r.idle_ms = set.idle_ms;
        key = key_of(ep);
        auto& p = r.pools[key];
        if (p.label.empty())
            p.label = label_of(ep);
        while (!p.idle.empty()) {
            auto c = p.idle.back();         // the most recently used is likeliest alive
            p.idle.pop_back();
            if (still_good(c)) {
                conn = c;
                ++p.busy;
                ++r.reuses;
                send(cmds, false);
                return;
            }
            close_conn(r, p, c);
            ++r.discarded;
        }
        if (r.total_open >= r.max_connections) {
            ++r.refusals;
            fail("too many RESP connections (resp.max_connections is "
                 + std::to_string(r.max_connections) + ")", false);
            return;
        }
        ++p.open;
        ++p.busy;
        ++r.total_open;
        ++r.connects;
        conn = std::make_shared<connection>(r.io, set.lim);
        connect();
    }

    void arm(uint32_t ms, const char* what) {
        timer.expires_after(std::chrono::milliseconds(ms));
        auto self = shared_from_this();
        std::string why = what;
        timer.async_wait([self, why](const asio::error_code& ec) {
            if (ec || self->finished)
                return;
            ++self->r.timeouts;
            // closing the socket cancels whatever read or write is outstanding; its
            // handler then finds the exchange finished and does nothing
            self->fail(why, true);
        });
    }

    void connect() {
        arm(set.connect_timeout_ms, "timed out connecting");
        auto self = shared_from_this();
        resolver.async_resolve(ep.host, std::to_string(ep.port),
            [self](const asio::error_code& ec, const tcp::resolver::results_type& results) {
                if (self->finished)
                    return;
                if (ec) {
                    self->fail("could not resolve " + self->ep.host + ": " + ec.message(), true);
                    return;
                }
                asio::async_connect(self->conn->sock, results,
                    [self](const asio::error_code& ec2, const tcp::endpoint&) {
                        if (self->finished)
                            return;
                        if (ec2) {
                            self->fail("could not connect to " + self->ep.host + ":"
                                       + std::to_string(self->ep.port) + ": " + ec2.message(), true);
                            return;
                        }
                        asio::error_code ignored;
                        self->conn->sock.set_option(tcp::no_delay(true), ignored);
                        self->handshake();
                    });
            });
    }

    /*
     * AUTH, HELLO and SELECT on a new connection, as an exchange of their own. Sending
     * them in front of the caller's commands would save a round trip, but a failed
     * SELECT would then let the caller's commands run against the wrong database.
     */
    void handshake() {
        std::vector<command> h;
        if (ep.protocol == 3) {
            command hello{"HELLO", "3"};
            if (!ep.password.empty()) {
                hello.push_back("AUTH");
                hello.push_back(ep.user.empty() ? "default" : ep.user);
                hello.push_back(ep.password);
            }
            h.push_back(std::move(hello));
        } else if (!ep.password.empty()) {
            if (ep.user.empty())
                h.push_back({"AUTH", ep.password});
            else
                h.push_back({"AUTH", ep.user, ep.password});
        }
        if (ep.db >= 0)
            h.push_back({"SELECT", std::to_string(ep.db)});
        if (h.empty()) {
            timer.cancel();
            send(cmds, false);
            return;
        }
        send(h, true);
    }

    void send(const std::vector<command>& what, bool handshaking) {
        in_handshake = handshaking;
        // this exchange's limits, not whatever the connection was opened with: a
        // pooled connection outlives the settings of the run that made it
        conn->parser.set_limits(set.lim);
        got.clear();
        expect = what.size();
        wire = encode(what);
        if (!handshaking)
            arm(set.timeout_ms, "timed out waiting for the reply");
        auto self = shared_from_this();
        asio::async_write(conn->sock, asio::buffer(wire),
            [self](const asio::error_code& ec, size_t) {
                if (self->finished)
                    return;
                if (ec) {
                    self->fail("could not send: " + ec.message(), true);
                    return;
                }
                self->drain();
            });
    }

    /** replies out of the parser until there are enough, reading when it runs dry */
    void drain() {
        while (got.size() < expect) {
            Variable v;
            std::string err;
            int res = conn->parser.next(v, err);
            if (res < 0) {
                fail("the server's reply isn't RESP: " + err, true);
                return;
            }
            if (res == 0) {
                read_more();
                return;
            }
            got.push_back(std::move(v));
        }
        if (in_handshake) {
            for (const auto& v : got) {
                if (v.index() == var_error) {
                    fail(std::string("the server refused the connection setup: ")
                         + std::get<error>(v).what(), true);
                    return;
                }
            }
            timer.cancel();
            send(cmds, false);
            return;
        }
        succeed();
    }

    void read_more() {
        auto self = shared_from_this();
        conn->sock.async_read_some(asio::buffer(rbuf),
            [self](const asio::error_code& ec, size_t n) {
                if (self->finished)
                    return;
                if (ec) {
                    self->fail(ec == asio::error::eof
                               ? std::string("the server closed the connection")
                               : "could not read: " + ec.message(), true);
                    return;
                }
                self->conn->parser.feed(self->rbuf.data(), n);
                self->drain();
            });
    }

    void succeed() {
        finished = true;
        timer.cancel();
        auto& p = r.pools[key];
        if (p.busy > 0)
            --p.busy;
        // leftovers mean the stream and the replies no longer line up
        if (!conn->parser.idle() || changes_session(cmds)) {
            close_conn(r, p, conn);
            ++r.discarded;
        } else {
            conn->idle_since = clock_t_::now();
            p.idle.push_back(conn);
            while (p.idle.size() > r.max_idle) {
                close_conn(r, p, p.idle.front());
                p.idle.pop_front();
            }
            arm_reaper(r);
        }
        conn.reset();
        auto d = std::move(done);
        d(std::move(got), std::string());
    }

    /** `had_conn`: a connection was checked out for this and has to be accounted for */
    void fail(const std::string& why, bool had_conn) {
        if (finished)
            return;
        finished = true;
        ++r.failures;
        timer.cancel();
        asio::error_code ignored;
        resolver.cancel();
        if (had_conn && conn) {
            auto& p = r.pools[key];
            if (p.busy > 0)
                --p.busy;
            close_conn(r, p, conn);
        }
        conn.reset();
        // a pool left with nothing in it goes now; the reaper only runs while
        // something is idle, and a server that never answers leaves nothing idle
        if (auto it = r.pools.find(key); it != r.pools.end() && it->second.open == 0
            && it->second.busy == 0 && it->second.idle.empty())
            r.pools.erase(it);
        auto d = std::move(done);
        d({}, "resp: " + why);
    }
};

}

void run(const endpoint& ep, const settings& s, std::vector<command> commands, done_fn done) {
    auto& r = the_reactor();
    auto x = std::make_shared<exchange>(r, ep, s, std::move(commands), std::move(done));
    asio::post(r.io, [x] { x->start(); });
}

std::string refused(const command& c) {
    if (c.empty())
        return "a command needs a name";
    auto w = upper(c[0]);
    if (w == "SUBSCRIBE" || w == "PSUBSCRIBE" || w == "SSUBSCRIBE" || w == "MONITOR"
        || w == "SYNC" || w == "PSYNC")
        return w + " never stops replying, so it can't go through a pooled connection";
    if (w == "QUIT")
        return "QUIT ends the connection; connections here are pooled and closed for you";
    return {};
}

std::vector<std::string> pool_status() {
    auto& r = the_reactor();
    auto ready = std::make_shared<std::promise<std::vector<std::string>>>();
    auto fut = ready->get_future();
    // read on the reactor thread, which is the only one that may. The reactor never
    // waits on anything, so waiting on it here can't deadlock
    asio::post(r.io, [&r, ready] {
        std::vector<std::string> out;
        for (const auto& kv : r.pools) {
            const auto& p = kv.second;
            out.push_back(p.label + " open=" + std::to_string(p.open) + " idle="
                          + std::to_string(p.idle.size()) + " busy=" + std::to_string(p.busy));
        }
        out.push_back("total open=" + std::to_string(r.total_open)
                      + " max_connections=" + std::to_string(r.max_connections)
                      + " connects=" + std::to_string(r.connects)
                      + " reuses=" + std::to_string(r.reuses)
                      + " discarded=" + std::to_string(r.discarded)
                      + " timeouts=" + std::to_string(r.timeouts)
                      + " refused=" + std::to_string(r.refusals)
                      + " failures=" + std::to_string(r.failures));
        ready->set_value(std::move(out));
    });
    if (fut.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
        return {"the RESP client's thread didn't answer"};
    return fut.get();
}

}

/* RESP POOL - what the Luau RESP client's pools hold. See TODO 379. */
int RESP(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    std::string sub(argv[1].chars(), argv[1].size);
    for (auto& ch : sub)
        ch = (char) toupper((unsigned char) ch);
    if (sub != "POOL")
        return call.push_error("RESP POOL");
    auto lines = barch::resp_client::pool_status();
    call.start_array();
    for (const auto& l : lines)
        call.push_string(l);
    return call.end_array();
}

void register_resp_api(function_map& r) {
    r["RESP"] = {::RESP, {"stats"}};
}
