// Raft groups over group_store, restarted from their files - TODO 610.
#include "cluster/raft_group.h"
#include "cluster/raft_auth.h"

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <openssl/ssl.h>
#include <openssl/x509_vfy.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using barch::cluster::raft_group;
using namespace nuraft;

/*
 * Someone who can reach a group's port but doesn't have the secret - TODO 620. It
 * sends one request as member `as`, signed with `secret` or not at all, and says
 * whether an answer came back.
 */
static bool send_forged(const std::string& endpoint, ptr<req_msg> req, const std::string& secret) {
    asio_service::options o;
    if (!secret.empty()) {
        o.write_req_meta_ = [secret](const asio_service_meta_cb_params& p) {
            return p.req_ ? barch::cluster::sign_request(secret, *p.req_) : std::string{};
        };
    }
    auto svc = cs_new<asio_service>(o);
    auto cli = svc->create_client(endpoint);
    std::atomic<int> answered{0};
    rpc_handler h = [&answered](ptr<resp_msg>& resp, ptr<rpc_exception>& e) {
        answered = resp && !e ? 1 : 2;
    };
    cli->send(req, h, 2000);
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!answered && std::chrono::steady_clock::now() < until)
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    cli.reset();
    svc->stop();
    for (int i = 0; i < 300 && svc->get_active_workers(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    return answered == 1;
}

/*
 * A self-signed certificate and its key, as `openssl req -x509` makes them, made
 * here: running the openssl command from a process under TSan doesn't work, and the
 * TLS section skipped itself there, which is where it's needed - TODO 629
 */
static bool make_certificate(const std::string& cert, const std::string& key) {
    EVP_PKEY* pk = EVP_RSA_gen(2048);
    X509* x = X509_new();
    bool ok = pk && x;
    if (ok) {
        X509_set_version(x, 2);
        ASN1_INTEGER_set(X509_get_serialNumber(x), 1);
        X509_gmtime_adj(X509_getm_notBefore(x), 0);
        X509_gmtime_adj(X509_getm_notAfter(x), 86400);
        X509_set_pubkey(x, pk);
        X509_NAME* name = X509_get_subject_name(x);
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, (const unsigned char*) "raftgrouptest", -1, -1, 0);
        X509_set_issuer_name(x, name);
        X509V3_CTX v3;
        X509V3_set_ctx_nodb(&v3);
        X509V3_set_ctx(&v3, x, x, nullptr, nullptr, 0);
        for (const auto& [nid, value] : {std::pair{NID_subject_key_identifier, "hash"},
                                         std::pair{NID_authority_key_identifier, "keyid:always"},
                                         std::pair{NID_basic_constraints, "critical,CA:TRUE"}}) {
            X509_EXTENSION* e = X509V3_EXT_conf_nid(nullptr, &v3, nid, value);
            ok = ok && e && X509_add_ext(x, e, -1);
            X509_EXTENSION_free(e);
        }
        ok = ok && X509_sign(x, pk, EVP_sha256()) > 0;
    }
    FILE* c = ok ? std::fopen(cert.c_str(), "w") : nullptr;
    FILE* k = ok ? std::fopen(key.c_str(), "w") : nullptr;
    ok = ok && c && k && PEM_write_X509(c, x) && PEM_write_PrivateKey(k, pk, nullptr, nullptr, 0, nullptr, nullptr);
    if (c) std::fclose(c);
    if (k) std::fclose(k);
    X509_free(x);
    EVP_PKEY_free(pk);
    return ok;
}

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-58s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    std::fflush(stdout);
    if (!ok) ++failures;
}

static bool wait_for(int ms, const std::function<bool()>& f) {
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < until) {
        if (f()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return f();
}

// the values in commit order; a commit answers with its index, as raft_group expects
class list_machine : public state_machine {
public:
    ptr<buffer> commit(const ulong idx, buffer& data) override {
        if (const int d = delay_ms.load())
            std::this_thread::sleep_for(std::chrono::milliseconds(d));
        buffer_serializer bs(data);
        const uint64_t v = bs.get_u64();
        {
            std::lock_guard l(m);
            values.push_back(v);
            last = idx;
        }
        auto out = buffer::alloc(sizeof(ulong));
        out->put(idx);
        out->pos(0);
        return out;
    }
    void commit_config(const ulong idx, ptr<cluster_config>&) override {
        std::lock_guard l(m);
        last = idx;
    }
    ulong last_commit_index() override {
        std::lock_guard l(m);
        return last;
    }
    ptr<snapshot> last_snapshot() override { return nullptr; }
    void create_snapshot(snapshot&, async_result<bool>::handler_type& done) override {
        ptr<std::exception> none;
        bool ok = false;
        done(ok, none);
    }
    bool apply_snapshot(snapshot&) override { return false; }
    std::vector<uint64_t> copy() {
        std::lock_guard l(m);
        return values;
    }
    // a slow apply, so a client's wait can end first
    std::atomic<int> delay_ms{0};
private:
    std::mutex m;
    std::vector<uint64_t> values;
    ulong last{0};
};

struct node {
    raft_group::options opt;
    ptr<list_machine> sm;
    std::unique_ptr<raft_group> g;

    bool start() {
        sm = cs_new<list_machine>();
        g = std::make_unique<raft_group>(opt, sm);
        std::string err;
        if (!g->start(err)) {
            std::printf("    %s\n", err.c_str());
            return false;
        }
        return true;
    }
    void stop() {
        if (g) g->stop();
        g.reset();
    }
};

static ptr<buffer> value(uint64_t v) {
    auto b = buffer::alloc(sizeof(uint64_t));
    buffer_serializer bs(b);
    bs.put_u64(v);
    return b;
}

static node* leader_of(std::vector<node>& nodes) {
    for (auto& n : nodes)
        if (n.g && n.g->is_leader()) return &n;
    return nullptr;
}

static bool all_hold(std::vector<node>& nodes, const std::vector<uint64_t>& want) {
    for (auto& n : nodes)
        if (n.g && n.sm->copy() != want) return false;
    return true;
}

static bool append_all(std::vector<node>& nodes, uint64_t from, uint64_t to, std::vector<uint64_t>& want) {
    for (uint64_t v = from; v <= to; ++v) {
        bool done = wait_for(5000, [&] {
            auto* l = leader_of(nodes);
            if (!l) return false;
            uint64_t idx = 0;
            std::string why;
            return l->g->append(value(v), idx, why) == raft_group::outcome::committed && idx > 0;
        });
        if (!done) return false;
        want.push_back(v);
    }
    return true;
}

int main() {
    namespace fs = std::filesystem;
    const char* p = std::getenv("BARCH_TEST_PORT");
    const int base = p && *p ? std::atoi(p) : 26200;
    const fs::path dir = fs::current_path() / "raftgrouptest.d";
    fs::remove_all(dir);

    // three nodes, each with its own raft_port and directory; group 1 on each
    std::vector<node> nodes(3);
    for (int i = 0; i < 3; ++i) {
        nodes[i].opt.group = 1;
        nodes[i].opt.dir = (dir / ("n" + std::to_string(i + 1))).string();
        nodes[i].opt.base_port = base + 2 * i;
        nodes[i].opt.server_id = i + 1;
        nodes[i].opt.join = i != 0;
        nodes[i].opt.secret = "raftgrouptest secret";
    }

    std::printf("a group of three, built one member at a time\n");
    check(nodes[0].start(), "node 1 starts the group");
    check(wait_for(5000, [&] { return nodes[0].g->is_leader(); }), "and leads it");
    check(nodes[1].start() && nodes[2].start(), "nodes 2 and 3 start, waiting to be added");
    check(!nodes[1].g->is_leader() && !nodes[2].g->is_leader(), "and don't lead a group of their own");
    std::string err;
    for (int i = 1; i < 3; ++i) {
        const bool added = nodes[0].g->add_member(nodes[i].opt.server_id, nodes[i].g->my_endpoint(), 10000, err);
        check(added, "node " + std::to_string(i + 1) + " is added" + (added ? "" : ": " + err));
    }
    check(nodes[0].g->members().size() == 3, "the leader lists three members");
    check(wait_for(5000, [&] { return nodes[2].g->members().size() == 3; }), "and so does the last one added");

    std::vector<uint64_t> want;
    check(append_all(nodes, 1, 40, want), "40 entries commit");
    check(wait_for(5000, [&] { return all_hold(nodes, want); }), "and every member applied them in order");

    std::printf("the leader stops\n");
    node* old = leader_of(nodes);
    const int32_t old_id = old ? old->g->my_id() : -1;
    if (old) old->stop();
    check(wait_for(10000, [&] {
              auto* l = leader_of(nodes);
              return l && l->g->my_id() != old_id;
          }),
          "the other two elect a new leader");
    check(append_all(nodes, 41, 60, want), "20 more commit on two of three");
    for (auto& n : nodes)
        if (n.opt.server_id == old_id) check(n.start(), "the old leader comes back from its file");
    check(wait_for(10000, [&] { return all_hold(nodes, want); }), "and catches up with all 60");

    std::printf("every member stops and starts again\n");
    for (auto& n : nodes) n.stop();
    bool started = true;
    for (auto& n : nodes) started = started && n.start();
    check(started, "all three start from their files");
    check(wait_for(10000, [&] { return leader_of(nodes) != nullptr; }), "elect a leader");
    check(leader_of(nodes) && leader_of(nodes)->g->members().size() == 3, "with the same three members");
    check(append_all(nodes, 61, 70, want), "take 10 more");
    check(wait_for(10000, [&] { return all_hold(nodes, want); }),
          "and every member holds all 70, the first 60 replayed from the log");

    std::printf("the port refuses what isn't signed with the secret\n");
    {
        node* lead = leader_of(nodes);
        node* target = nullptr;
        for (auto& n : nodes)
            if (n.g && &n != lead) { target = &n; break; }
        check(lead && target, "a leader and a follower to aim at");
        if (lead && target) {
            const uint64_t term = target->g->term();
            const uint64_t refused = target->g->refused_messages();
            // a vote request posing as the leader, with a term far ahead: taken,
            // it would move the follower's term and unseat the leader
            auto forge = [&](uint64_t t) {
                return cs_new<req_msg>(t, msg_type::request_vote_request, lead->g->my_id(),
                                       target->g->my_id(), term, 1000000, 0);
            };
            const bool unsigned_answered = send_forged(target->g->my_endpoint(), forge(term + 100), "");
            const bool wrong_answered = send_forged(target->g->my_endpoint(), forge(term + 100), "not the secret");
            check(!unsigned_answered, "an unsigned request gets no answer");
            check(!wrong_answered, "nor does one signed with the wrong secret");
            check(target->g->refused_messages() >= refused + 2,
                  "the follower counts both as refused (" + std::to_string(target->g->refused_messages() - refused) + ")");
            check(target->g->term() == term, "and its term didn't move (" + std::to_string(target->g->term()) + ")");
            // at the term it has, so taking it changes nothing
            check(send_forged(target->g->my_endpoint(), forge(term), "raftgrouptest secret"),
                  "while one signed with the secret is answered");
        }
    }
    {
        node stranger;
        stranger.opt = nodes[0].opt;
        stranger.opt.dir = (dir / "nosecret").string();
        stranger.opt.base_port = base + 6;
        stranger.opt.server_id = 9;
        stranger.opt.secret.clear();
        check(!stranger.start(), "a group without a secret doesn't start");
    }

    std::printf("a client that stops waiting before its entry commits\n");
    {
        // the wait ends after 1ms and each apply takes 3, so every append answers
        // before its entry commits, and raft_group has to call that unknown, not
        // refused: the entry is in the log and commits anyway. This is where TODO
        // 623's race in NuRaft lives, but it doesn't make it happen - the window
        // is a few microseconds between two of NuRaft's locks
        node hasty;
        hasty.opt = nodes[0].opt;
        hasty.opt.dir = (dir / "hasty").string();
        hasty.opt.base_port = base + 8;
        hasty.opt.server_id = 1;
        hasty.opt.join = false;
        hasty.opt.client_timeout_ms = 1;
        check(hasty.start() && wait_for(5000, [&] { return hasty.g->is_leader(); }), "a group of one leads");
        hasty.sm->delay_ms = 3;
        int committed = 0, unknown = 0, refused = 0;
        std::vector<uint64_t> sent;
        for (uint64_t v = 1000; v < 1100; ++v) {
            uint64_t idx = 0;
            std::string why;
            switch (hasty.g->append(value(v), idx, why)) {
                case raft_group::outcome::committed: ++committed; sent.push_back(v); break;
                case raft_group::outcome::unknown: ++unknown; sent.push_back(v); break;
                case raft_group::outcome::refused: ++refused; break;
            }
        }
        std::printf("    %d committed, %d unknown, %d refused\n", committed, unknown, refused);
        check(unknown > 0, "some answer before their entry commits");
        check(refused == 0, "none is refused");
        check(wait_for(10000, [&] { return hasty.sm->copy() == sent; }),
              "and every one of them commits, in order");
        hasty.stop();
    }

    std::printf("TLS contexts, checked from many threads at once\n");
    {
        // TODO 629: verifying peers against a context's CA from several threads is
        // what handshakes do, and OpenSSL used to fill the CA's cached fields in
        // under one of them while another read them. Under TSan that's a report,
        // and the test's exit code says so; here it also checks they verify
        const std::string cert = (dir / "tls.crt").string(), key = (dir / "tls.key").string();
        check(make_certificate(cert, key), "a self-signed certificate to check against");
        {
            std::atomic<int> verified{0}, failed{0};
            // a cold CA showed the race in 50 rounds and 200, but not always in 30
            for (int round = 0; round < 200; ++round) {
                SSL_CTX* cli = barch::cluster::tls_client_context(cert);
                std::vector<std::thread> ts;
                for (int i = 0; i < 8; ++i)
                    ts.emplace_back([&] {
                        FILE* f = std::fopen(cert.c_str(), "r");
                        X509* peer = f ? PEM_read_X509(f, nullptr, nullptr, nullptr) : nullptr;
                        if (f) std::fclose(f);
                        X509_STORE_CTX* c = X509_STORE_CTX_new();
                        X509_STORE_CTX_init(c, SSL_CTX_get_cert_store(cli), peer, nullptr);
                        (X509_verify_cert(c) == 1 ? verified : failed)++;
                        X509_STORE_CTX_free(c);
                        X509_free(peer);
                    });
                for (auto& t : ts) t.join();
                SSL_CTX_free(cli);
            }
            check(failed == 0 && verified == 1600,
                  "eight threads verify a peer against it, 200 times (" + std::to_string(verified) + " ok)");
            SSL_CTX* srv = barch::cluster::tls_server_context(cert, key);
            check(srv != nullptr, "the server context loads the same certificate and key");
            SSL_CTX_free(srv);
            bool threw = false;
            try {
                barch::cluster::tls_client_context((dir / "missing.crt").string());
            } catch (const std::exception& e) {
                threw = std::string(e.what()).find("missing.crt") != std::string::npos;
            }
            check(threw, "and a file that isn't there is an error that names it");
        }
    }

    std::printf("a member is removed\n");
    node* l = leader_of(nodes);
    node* other = nullptr;
    for (auto& n : nodes)
        if (&n != l) { other = &n; break; }
    check(l && other && l->g->remove_member(other->g->my_id(), 10000, err), "the leader removes one");
    check(l && l->g->members().size() == 2, "and lists two");
    other->stop();
    check(append_all(nodes, 71, 75, want), "the two left still commit");

    for (auto& n : nodes) n.stop();
    fs::remove_all(dir);
    std::printf("%s\n", failures ? "FAILED" : "all passed");
    return failures ? 1 : 0;
}
