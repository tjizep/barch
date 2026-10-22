//
// Created by teejip on 6/6/25.
//

#ifndef SWIG_CALLER_H
#define SWIG_CALLER_H
#include <cctype>
#include <chrono>
#include <thread>
#include "caller.h"
#include <set>
#include <optional>
#include <string>
#include <vector>
#include "keys.h"
#include "module.h"
#include "rpc/server.h"
#include "barch_apis.h"
#include "connection_api.h"
#include "sastam.h"
#include "auth_api.h"
#include "statistics.h"
#include "rpc/barch_functions.h"
#include "rpc/redis_parser.h"
#include "vector_stream.h"
#include "cluster_hooks.h"
#include "raft_writes.h"
#include "tx_session.h"
#include "spaces_spec.h"
#include "function_api.h"

/** what a command run from the log answered, as the leader's client gets it - TODO 669 */
struct rpc_reply : barch::cluster::command_reply {
    heap::vector<Variable> results{};
    heap::vector<std::string> errors{};
    int code{0};
    [[nodiscard]] std::string error() const override {
        if (!errors.empty())
            return errors.front();
        return code != 0 ? std::string("ERR the command failed") : std::string();
    }
    [[nodiscard]] std::string result() const override {
        return results.empty() ? std::string() : results.front().s();
    }
};

struct rpc_caller : caller {
    // before any member goes: a call interface may be using this - see caller::guard
    ~rpc_caller() override { revoke_guard(); }
    barch::key_space_ptr ks {get_default_ks()};
    /**
     * built here rather than on first use, because an asynchronous call runs against a
     * *copy* of this caller. A cache made inside that copy would be thrown away with
     * it, and since every function call is asynchronous the cache would never survive
     * a single call. Allocated up front, both share the one holder.
     */
    barch::foreign::function_states_ptr luau_states {barch::foreign::make_function_states()};
    std::shared_ptr<barch::repl::rpc> host {};
    heap::vector<std::shared_ptr<barch::repl::rpc>> routes {};
    size_t valid_routes{};
    std::string r{};
    heap::vector<Variable> results{};
    heap::vector<heap::vector<wrapped_variable_t>> temp{};
    heap::vector<std::string> errors{};
    heap::vector<bool> acl{get_all_acl()};
    /**
     * per space rights, and the one worked out for the space in use.
     *
     * Resolved when the space changes rather than per command: it is a map lookup and
     * a vector copy, and a pipeline of commands in one space should pay for it once.
     */
    barch::space_overrides space_acl{};
    std::string acl_space{"\x01none"};
    heap::vector<bool> effective_acl{};
    heap::vector<bool> named_acl{};
    /**
     * The interfaces this connection's calls reach, most recent first - TODO 430.
     *
     * One slot used to be rebuilt, and the old interface freed, every time the
     * connection ran a call in another space, so a connection going back and forth
     * between two spaces built one per call - the pattern TODO 427 crashed on. A few
     * of them, keyed by the spaces they were built for, keep both. set_acl drops them
     * all, since they were built from the rights it replaces.
     */
    heap::vector<barch::foreign::call_interface_ptr> script_ifaces{};
    static constexpr size_t max_script_ifaces = 4;
    /** see caller::script_depth */
    int nest_depth{0};
    /** see caller::resolutions - names already known to be functions here */
    heap::string_map<caller::resolved> resolved{};
    std::string resolved_space{"\x01none"};
    arg_t args{};
    std::string user = "default";
    std::function<std::string()> info_fun;
    bool call_buffering = false;
    keys_t blocks{};
    /**
     * whether this caller is counted in statistics::blocked_clients. The count is of
     * parked sessions, not of the keys they are waiting on, and park and unpark are
     * not perfectly paired - erase_blocks is called on the disconnect path too - so
     * the flag is what keeps it from drifting either way
     */
    bool counted_blocked = false;
    Variable empty{nullptr};
    std::function<void(caller&, const keys_t&)> block_fun;
    /** see caller::after_blocks_registered */
    std::function<void()> blocks_registered_fun;
    uint64_t block_to_ms = 0;
    heap::vector<Variable> buffered_results{};
    heap::vector<std::string> buffered_errors{};
    bool remote {true};
    // the RESP version this connection settled on with HELLO. the caller lives for the
    // life of the session, so the negotiation sticks for every command that follows
    int protocol {2};

    // the session sets this to its streamed write, which queues the bytes on the
    // socket and may wait for the client to take some. null means the reply stays
    // in results and is written after the call, as it always was.
    std::function<bool(const char*, size_t)> write_socket_bytes;
    // only a call running on the worker pool may stream - TODO 488. A streamed
    // write can wait on the socket, and waiting on the socket's own thread would
    // stop the very write it's waiting for. The session sets it on the copy it
    // hands to a worker, never on its own caller.
    bool stream_to_socket{false};
    // the session's reply buffer for this call. GET writes a bulk string
    // here from the leaf so the value is not copied into results first
    vector_stream* reply_out{nullptr};
    // true once write_socket* has put bytes on the socket. write_result must
    // not emit a second reply (an empty results vector is a RESP null).
    bool reply_sent{false};
    // EXEC collects inner replies as Variables. KEYS must not write the socket
    // then, or the items leave the EXEC array.
    bool collecting_exec{false};
    /**
     * How a key lock someone else holds is met - TODO 721. `lock_wait_here` waits on this
     * thread, as anything that can't park does. `lock_park`, which a RESP session sets
     * on its caller, parks the session instead, the way BLPOP does, so its I/O thread
     * goes on serving the others on it, the lock's holder perhaps. `lock_retry` is a
     * parked command run again once woken: it only tries, and says in `lock_busy` if
     * the key is still held.
     */
    enum class lock_wait { here, park, retry };
    lock_wait lock_mode{lock_wait::here};
    barch::tx_lock_busy lock_busy{};
    /** `lock_busy` is the transaction's lock, rather than one a plain write waits on */
    bool lock_busy_in_tx{false};
    /**
     * Parks here, rather than waiting: not inside EXEC or a script's call. A script's
     * call that the script can park on says so with `lock_retry` - TODO 731.
     */
    [[nodiscard]] bool lock_parks() const {
        return lock_mode == lock_wait::retry || (lock_mode == lock_wait::park && nest_depth == 0 && !collecting_exec);
    }

    [[nodiscard]] const barch::foreign::function_states_ptr& function_states() const override {
        return luau_states;
    }

    [[nodiscard]] bool is_collecting_exec() const override {
        return collecting_exec;
    }

    [[nodiscard]] bool can_write_socket() const override {
        return stream_to_socket && (bool) write_socket_bytes && !call_buffering && !collecting_exec;
    }

    bool write_encoded(const vector_stream& encoded) {
        if (encoded.empty()) {
            reply_sent = true;
            return true;
        }
        if (!write_socket_bytes((const char*) encoded.buf.data(), encoded.buf.size())) {
            return false;
        }
        reply_sent = true;
        return true;
    }

    void encode_socket_item(vector_stream& out, const Variable& v) override {
        redis::rwrite(out, v, protocol);
    }

    bool write_socket_array(size_t n, const vector_stream& items) override {
        if (!can_write_socket()) return false;
        vector_stream header;
        redis::rwrite_header(header, '*', n);
        if (!write_encoded(header)) return false;
        return write_encoded(items);
    }

    void create(const std::string& h, uint_least16_t port) {
        this->host = barch::repl::create(h,port);
    }
    rpc_caller(const rpc_caller& caller) = default;
    rpc_caller() {
        set_context(ctx_swig);
        update_routes();
        std::vector<std::string_view> auth = {"AUTH","default","empty"};
        if (this->call( auth,::AUTH) != 0) {
            barch::err({"could not authenticate `default`"});
        }
    }
    Variable retval(int r, Variable def) {
        if (!errors.empty()) {
            return ::error{errors[0]};
        }
        if (r != ok()) {
            return Variable{::error{"there was an error"}};
        }
        if (results.empty()) {
            return def;
        }
        return results[0];
    }
    // end_array keeps an array reply as one nested value so the writer does not have
    // to guess the shape. the language bindings were written against the flat list
    // that used to be spliced into results, so these hand them that same view.
    [[nodiscard]] bool nested_result() const {
        return results.size() == 1 && results[0].index() == var_array;
    }
    [[nodiscard]] const heap::vector<wrapped_variable_t>& nested_results() const {
        return std::get<heap::vector<wrapped_variable_t>>((const variable_t&) results[0]);
    }
    [[nodiscard]] size_t flat_size() const {
        return nested_result() ? nested_results().size() : results.size();
    }
    [[nodiscard]] bool flat_empty() const {
        return flat_size() == 0;
    }
    [[nodiscard]] Variable flat_at(size_t i) const {
        if (nested_result()) return Variable{nested_results()[i].var};
        return results[i];
    }
    template<typename Out>
    void append_flat(Out& out) const {
        size_t n = flat_size();
        for (size_t i = 0; i < n; ++i) out.emplace_back(flat_at(i));
    }

    [[nodiscard]] size_t results_count() const final {
        if (temp.empty())
            return results.size();
        return temp.back().size();
    };
    [[nodiscard]] size_t errors_count() const final {
        return errors.size();
    }

    [[nodiscard]] bool is_remote() const override {
        return remote;
    }
    /**
     * The first word of a redis error is a code, not part of the sentence.
     *
     * Clients read it: redis-py maps ERR, WRONGTYPE, NOAUTH and the rest onto exception
     * classes, so an error without a recognised code arrives as a bare ResponseError and
     * anything branching on the type gets it wrong. barch's messages were plain phrases -
     * `Wrong Arity`, `Syntax Error`, `no such key` - so none of them carried one.
     *
     * A message that already begins with an all upper case word is left alone, because
     * that word is its code; anything else is a sentence and gets ERR in front of it.
     */
    static std::string with_error_code(const std::string& message) {
        auto end = message.find(' ');
        auto head = end == std::string::npos ? message : message.substr(0, end);
        if (head.size() >= 3) {
            bool code = true;
            for (auto ch : head) {
                if (!std::isupper((unsigned char) ch)) { code = false; break; }
            }
            if (code) return message;
        }
        return "ERR " + message;
    }

    [[nodiscard]] int wrong_arity()  override {
        // named, the way redis names it. args[0] is the command as the client sent it,
        // and redis reports it lower cased whatever case it arrived in
        std::string name = args.empty() ? std::string() : args[0].to_string();
        for (auto& ch : name) ch = (char) std::tolower((unsigned char) ch);
        if (name.empty()) {
            errors.emplace_back("ERR wrong number of arguments");
        } else {
            errors.emplace_back("ERR wrong number of arguments for '" + name + "' command");
        }
        return 0;
    }
    [[nodiscard]] int syntax_error() override {
        errors.emplace_back("ERR syntax error");
        return 0;
    }
    [[nodiscard]] int error() const override {
        return -1;
    }
    [[nodiscard]] int push_error(const char * e) override {
        errors.emplace_back(with_error_code(e));
        return 0;
    }
    int key_check_error(art::value_type k) override {
        if (k.empty()) {
            return this->push_error("Key should not be empty");
        }
        //if (strnlen(k.chars(), k.size) < k.size) {
            //return this->push_error("No keys with embedded nulls");
        //}
        return this->push_error("Unspecified key error");
    }
    int push_null() override {
        emplace_impl(nullptr);
        return 0;
    }
    [[nodiscard]] int ok() const override {
        return 0;
    }
    int push_bool(bool value) override {
        emplace_impl(value);
        return 0;
    }
    int push_ll(int64_t l) override {
        emplace_impl(l);
        return 0;
    }
    int push_int(caller_other_int l) override {
        emplace_impl((int64_t) l);
        return 0;
    }
    int push_int(caller_other_uint l) override {
        emplace_impl((uint64_t) l);
        return 0;
    }
    int push_int(int64_t l) override {
        emplace_impl(l);
        return 0;
    }
    int push_int(uint64_t l) override {
        emplace_impl(l);
        return 0;
    }
    int push_int(int32_t l) override {
        emplace_impl((int64_t)l);
        return 0;
    }
    int push_int(uint32_t l) override {
        emplace_impl((uint64_t)l);
        return 0;
    }
    template<typename IT>
    int set_impl(size_t at, IT l) {
        if (!temp.empty()) {
            if (at >= temp.back().size()) return this->error();
            temp.back()[at] = l;
            return this->ok();
        }
        if (at >= results.size()) {
            throw_exception<std::runtime_error>("set index out of range");
            return this->error();
        }
        results[at] = l;
        return this->ok();
    }
    int set_int(size_t at, caller_other_int l) final {
       return set_impl(at, (int64_t) l);
    }
    int set_int(size_t at, caller_other_uint l)  final {
        return set_impl(at, (uint64_t) l);
    }
    int set_int(size_t at, int64_t l)  final {
        return set_impl(at, l);
    }
    int set_int(size_t at, uint64_t l)  final {
        return set_impl(at, l);
    }
    int set_int(size_t at, int32_t l)   final {
        return set_impl(at, l);
    }
    int set_int(size_t at, uint32_t l)   final {
        return set_impl(at, (int64_t)l);
    }
    template<typename T>
    int to_array_impl(T&into, size_t from) {
        heap::vector<wrapped_variable_t> arr;
        for (size_t at = from; at < into.size(); ++at) {
            arr.emplace_back(into.at(at));
        }
        into.resize(from + 1);
        into[from] = arr;
        return ok();
    }
    int to_array(size_t from) final {
        if (!temp.empty()) {
            return to_array_impl(temp.back(), from);
        }else {
            return to_array_impl(results, from);
        }
    }
    template<typename T>
    void emplace_impl(const T& val) {
        if (!temp.empty()) {
            temp.back().emplace_back(val);
        }else {
            results.emplace_back(val);
        }
    }
    int push_double(double l) override {
        emplace_impl(l);
        return 0;
    }

    template<typename T>
    void push_vt_impl(T& into, art::value_type v, bool dollar = false) {
        into.emplace_back(std::string{});
        auto& s = std::get<std::string>((variable_t&)into.back()); // values are currently always a string
        if (dollar) s.push_back('$');
        s.insert(s.end(), v.begin(), v.end());

    }

    int push_vt(art::value_type v) override {
        if (!temp.empty())
            push_vt_impl(temp.back(), v, true);
        else
            push_vt_impl(results, v, true);
        return 0;
    }

    int push_bulk(art::value_type v) override {
        if (!temp.empty() || collecting_exec || call_buffering || !reply_out) {
            return push_vt(v);
        }
        redis::rwrite_bulk(*reply_out, v.chars(), v.size);
        reply_sent = true;
        return 0;
    }

    int push_simple(art::value_type v) override {
        if (!temp.empty())
            push_vt_impl(temp.back(), v, false);
        else
            push_vt_impl(results, v, false);
        return 0;
    }

    int push_simple(const char * v) override {
        emplace_impl(v);
        return 0;
    }
    int push_simple(const std::string& v) override {
        emplace_impl(v);
        return 0;
    }

    int start_array() override {
        temp.emplace_back();
        return 0;
    }
    enum aggregate_kind { aggregate_array, aggregate_map, aggregate_set };
    /**
     * close the aggregate on top of the stack into whichever value its kind calls for.
     * it is always kept as one nested value: it used to be spliced into results when it
     * was the whole reply, which threw away the fact that it was an aggregate at all -
     * the writer then had to guess from the element count and got it wrong for one
     * holding nothing or one thing.
     */
    int close_aggregate(aggregate_kind kind) {
        if (temp.empty()) return this->error();

        auto b = temp.back();
        temp.pop_back();
        variable_t closed;
        switch (kind) {
            case aggregate_map: {
                map_t m;
                m.items = b;
                closed = m;
                break;
            }
            case aggregate_set: {
                set_t s;
                s.items = b;
                closed = s;
                break;
            }
            default:
                closed = b;
                break;
        }
        if (!temp.empty()) temp.back().emplace_back(closed);
        else results.emplace_back(closed);

        return 0;
    }
    int end_array() override {
        return close_aggregate(aggregate_array);
    }
    // a map and a set are built exactly like an array and only differ in the value they
    // are closed into, which is what tells the writer which RESP3 type to use
    int start_map() override {
        return start_array();
    }
    int end_map() override {
        return close_aggregate(aggregate_map);
    }
    int start_set() override {
        return start_array();
    }
    int end_set() override {
        return close_aggregate(aggregate_set);
    }
    bool pop_value(Variable& into) override {
        // an aggregate under construction owns the tail, otherwise it is the reply itself
        if (!temp.empty()) {
            auto& top = temp.back();
            if (top.empty()) return false;
            into = Variable((const variable_t&) top.back());
            top.pop_back();
            return true;
        }
        if (results.empty()) return false;
        into = results.back();
        results.pop_back();
        return true;
    }
    int push_verbatim(art::value_type v, const char* format = "txt") override {
        verbatim_t verbatim;
        verbatim.format = format;
        verbatim.text.assign(v.chars(), v.size);
        emplace_impl(verbatim);
        return 0;
    }
    [[nodiscard]] int get_protocol() const override {
        return protocol;
    }
    void set_protocol(int version) override {
        protocol = version;
    }
    int push_encoded_key(art::value_type key) override {
        char sep = ' ';
        if (ks && ks->key_split.size() == 1)
            sep = ks->key_split[0];
        emplace_impl(encoded_key_as_variant(key, sep));
        return 0;
    }

    int set_string(size_t at, const std::string& value) final {
        return set_impl(at, "$"+value);
    }

    int push_string(const std::string& v) override {
        emplace_impl("$"+v);
        return 0;
    }

    int push_values(const std::initializer_list<Variable>& keys) override {
        for (auto &k : keys) {
            emplace_impl(k);
        }
        return 0;
    };
    int push(const Variable & v) final {
        if (!temp.empty()) {
            temp.back().emplace_back(v);
            return ok();
        }
        results.emplace_back(v);
        return ok();
    }
    std::string convert(const std::string& v) {
        return v;
    }
    std::string convert(const std::string_view& v) {
        return {v.data(),v.size()};
    }
    std::string convert(const art::value_type& v) {
        return {v.chars(),v.size};
    }
    template<typename T>
    size_t pop_back_impl(T& a, size_t n ) {
        size_t popped = 0;
        while (!a.empty() && popped < n) {
            a.pop_back();
            ++popped;
        }
        return popped;
    }
    size_t pop_back(size_t n) override {
        if (!temp.empty()) {
            return pop_back_impl(temp.back(), n);
        }
        return pop_back_impl(results, n);
    };
    [[nodiscard]] size_t stack() const override {
        return results.size();
    }
    [[nodiscard]] const Variable& back() const override {
        if (!results.empty()) {
            return results.back();
        }
        return empty;
    }
    void update_routes() {
        if (ks != nullptr) {
            routes.resize(ks->get_shard_count());
            valid_routes = 0;
            for (size_t shard = 0; shard < ks->get_shard_count(); ++shard) {
                auto route = barch::repl::get_route(shard);
                if (route.ip.empty()) {
                    routes[shard] = nullptr;
                }else {
                    ++valid_routes;
                    routes[shard] = barch::repl::create(route.ip,route.port);
                }
            }
        }
    }
    template<typename ArgT>
    barch::repl::call_result call_route(const ArgT& params) {
        if (valid_routes && params.size() > 1) {
            size_t shard = kspace()->get_shard_index(params[1]);
            if (shard < routes.size()) {
                if (routes[shard] != nullptr) { // dont do any lookups if there's no route for perf
                    auto fbn = barch::barch_functions;
                    std::string n = convert(params[0]);
                    auto fi = fbn->find(n);
                    if (fi != fbn->end() && fi->second.is_data()) { // only route data calls
                        ++statistics::repl::attempted_routes;
                        auto cr = routes[shard]->call(results, params);
                        if (cr.net_error == 0) {
                            ++statistics::repl::routes_succeeded;
                            return cr;
                        } else {
                            // TODO: should we => if the data route network fails we will continue with other functions
                            routes[shard] = nullptr;
                            return cr;
                        }
                    }
                }
            }
        }
        return {-1,-1};
    }
    call_type fexec = EXEC;
    commands_t commands;
    /**
     * Read a Raft space here even when this node doesn't lead it - TODO 610.
     * The cluster's own bookkeeping reads its local copy of `cluster` this way;
     * clients never get it.
     */
    bool raft_local_reads{false};
    /**
     * A read that touches one key, so a space split over groups checks it against
     * that key's group alone, at its shard - TODO 615. The hash, list and ordered
     * set reads take their key first, all but the four that take several; and the
     * plain key reads named here. Every other read is checked against every group.
     */
    static bool single_key_read(const std::string& name) {
        static const std::set<std::string> several = {"ZDIFF", "ZUNION", "ZINTER", "ZINTERCARD"};
        static const std::set<std::string> plain = {"GET", "GETFORUPDATE", "GETRANGE", "SUBSTR", "STRLEN",
                                                    "TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME"};
        if (several.count(name)) return false;
        if (plain.count(name)) return true;
        auto fi = barch::barch_functions->find(name);
        if (fi == barch::barch_functions->end()) return false;
        const auto& m = get_category_map();
        for (const char* family : {"hash", "list", "orderedset"})
            if (fi->second.cats[m.at(family)]) return true;
        return false;
    }
    /**
     * follower reads and the indexes this connection has seen - TODO 613. Shared by
     * the copies a session makes for calls it runs off its own thread (TODO 626),
     * so a write that commits on a copy moves the connection's index, and a
     * CLUSTER AFTER on one is seen by the next. A connection's calls run one at a
     * time, so the copies never touch it at once.
     */
    std::shared_ptr<barch::cluster::session> raft_session = std::make_shared<barch::cluster::session>();
    barch::cluster::session* cluster_session() override { return raft_session.get(); }
    /** this connection's transaction - TODO 693 */
    // shared by the copies a Raft write runs on (asynch_call_context), as raft_session
    // is, so a transaction's writes and its COMMITTX reach the one session - TODO 693
    std::shared_ptr<barch::tx_session> tx_state = std::make_shared<barch::tx_session>();
    barch::tx_session* transaction() override { return tx_state.get(); }
    std::shared_ptr<barch::tx_session> shared_transaction() override { return tx_state; }
    /**
     * A script's caller takes its connection's transaction for one call, so a BEGINTX
     * it sends opens the connection's own, which the client sees and ends - TODO 705.
     * Null puts back a session of its own, closed.
     */
    void use_transaction(std::shared_ptr<barch::tx_session> t) {
        tx_state = t ? std::move(t) : std::make_shared<barch::tx_session>();
    }
    /**
     * is this argument that command name, whatever case it was sent in?
     *
     * The dispatcher folds the name it looks the function up by, but the parameters keep
     * the case the client wrote. Comparing them as they came meant `exec` in lower case
     * was not EXEC, so it was queued into the transaction it was supposed to end - and
     * every command after it was queued too, forever. Pipelining `EXEC` in upper case
     * worked, which is why the wire tests looked fine. See DONE 125
     */
    template<typename P>
    static bool name_is(const P& param, const char* name) {
        size_t n = 0;
        while (name[n]) ++n;
        if (param.size() != n) return false;
        for (size_t i = 0; i < n; ++i) {
            if (std::toupper((unsigned char) param[i]) != (unsigned char) name[i]) return false;
        }
        return true;
    }
    /** a command name as sent, upper case, with any `space:` prefix taken off */
    template<typename P>
    static std::string upper_name(const P& param) {
        std::string name(param.size(), '\0');
        for (size_t i = 0; i < param.size(); ++i)
            name[i] = (char) std::toupper((unsigned char) param[i]);
        if (const auto colon = name.find_last_of(':'); colon != std::string::npos && colon + 1 < name.size())
            name = name.substr(colon + 1);
        return name;
    }
    template<typename TC, typename VT>
    int call(const VT& params, TC&& f) {
        if (params.empty()) {
            barch::err({"invalid parameters"});
            return 0;
        }
        if (is_buffering() && !name_is(params[0], "EXEC")) {
            commands.emplace_back(f, params, ks);
            // redis answers +QUEUED for a command it has taken into a transaction. This
            // used to return with nothing pushed, and an empty results vector is written
            // as a RESP null - so every queued command answered $-1, and a client that
            // checks for QUEUED (redis-py's pipeline does) saw a transaction it could
            // not believe in. See DONE 123
            results.clear();
            return push_simple("QUEUED");
        }
        ++statistics::local_calls;
        args.clear();
        errors.clear();
        results.clear();
        temp.clear();
        reply_sent = false;
        bool raft_reading = false;
        /*
         * A name starting with \x01 is one of the server's own log entries (a RELOAD's
         * publish, a queue's push or settle - TODO 682). No command is called that, and
         * in a replicated space an unknown name goes to the log as a stored function,
         * where the runner would take it for the real thing.
         */
        if (!params.empty() && params[0].size() > 0 && params[0][0] == '\x01') {
            errors.emplace_back("ERR unknown command");
            return -1;
        }
        /*
         * Inside BEGINTX - TODO 693, 701: the commands a transaction takes run on its
         * overlay, a space that DEPENDS on the one it's in, so they read its own writes
         * first and the store beneath for the rest (which the read log keeps for the
         * prepare to check), and their writes stay in the overlay until COMMITTX.
         * Anything else is refused, since it would either run outside the transaction or
         * read what the transaction can't check.
         */
        bool tx_layer = false;
        bool tx_function = false;       // a stored function joining the transaction
        // what it's running, for INFO transactions, until this returns - TODO 720
        std::optional<barch::tx_running_scope> tx_running;
        if (tx_state->open && !barch::functions::running_from_log()) {
            const auto name = upper_name(params[0]);
            std::string tx_fname;
            // past a limit, or open too long: it can only be ended - TODO 701
            if (name != "BEGINTX" && name != "COMMITTX" && name != "ABORTTX") {
                const auto& why = barch::tx_before(*tx_state);
                if (!why.empty()) {
                    errors.emplace_back(why + ": ABORTTX or COMMITTX ends it");
                    return -1;
                }
            }
            // MULTI and EXEC hold the commands back; each is checked as EXEC runs it - TODO 710
            if (name != "BEGINTX" && name != "COMMITTX" && name != "ABORTTX" && name != "MULTI" && name != "EXEC") {
                // a lease for something outside the store, which a transaction doesn't
                // cover, and in one it would land only at COMMITTX - TODO 704
                if (name == "LOCK" || name == "UNLOCK") {
                    errors.emplace_back("ERR " + name + " isn't taken in a transaction: a transaction isolates its"
                                        " own keys already, and a lock for something outside the store is taken"
                                        " before BEGINTX and let go after COMMITTX");
                    return -1;
                }
                /*
                 * A stored function joins the transaction - TODO 707, 697: it runs as
                 * itself, and its barch.call and barch.store take the session's
                 * transaction. Not one a Raft space runs from its log, which every member
                 * would run outside the transaction.
                 */
                const auto named = barch::barch_functions->find(name);
                const bool function = name == "CALLF" || named == barch::barch_functions->end();
                if (function) {
                    if (ks != tx_state->space) {
                        errors.emplace_back("ERR a transaction writes the space it began in");
                        return -1;
                    }
                    const std::string fname = name == "CALLF" ? (params.size() > 1 ? convert(params[1]) : std::string())
                                                              : convert(params[0]);
                    if (ks->has_raft()
                        && barch::functions::replication_of(ks, fname) == barch::functions::replication::log) {
                        errors.emplace_back("ERR a function a Raft space runs from its log can't join a transaction, "
                                            "since every member runs it: give it \"replicate\": \"effects\" "
                                            "(TODO 697)");
                        return -1;
                    }
                    // in a pessimistic one its barch.call commands and barch.store calls take
                    // the same locks the client's would - TODO 732
                    tx_function = true;
                    tx_fname = fname;
                } else if (barch::tx_walks(name)) {
                    // over the snapshot's layers, a Raft space's too - TODO 701
                    if (tx_state->pessimistic) {
                        errors.emplace_back("ERR " + name + " isn't taken in a pessimistic transaction yet: it locks"
                                            " keys, not ranges, so a key written into the range later would be"
                                            " missed (TODO 719)");
                        return -1;
                    }
                } else if (!barch::tx_takes(name)) {
                    errors.emplace_back("ERR " + name + " isn't taken in a transaction yet: the string reads"
                                        " and writes are (GET, MGET, GETFORUPDATE, MGETFORUPDATE, EXISTS, STRLEN,"
                                        " SET, SETNX, SETEX, PSETEX,"
                                        " GETSET, MSET, DEL, INCR, DECR, INCRBY, DECRBY, APPEND), and RANGE,"
                                        " COUNT, KEYS and SCAN");
                    return -1;
                }
                if (!function) {
                    if (ks != tx_state->space) {
                        errors.emplace_back("ERR a transaction writes the space it began in");
                        return -1;
                    }
                    tx_layer = true;
                }
            }
            if (tx_state->status)
                tx_running.emplace(tx_state->status, tx_fname, name);
            /*
             * BEGINTX PESSIMISTIC - TODO 719: its keys locked before it runs, holding no
             * latch, so the one in the way can finish. A lock it can't get in
             * tx_lock_wait_ms leaves the transaction able only to end.
             */
            if (tx_layer && tx_state->pessimistic) {
                std::vector<std::string> as_sent;
                as_sent.reserve(params.size());
                for (const auto& p : params)
                    as_sent.push_back(convert(p));
                // parked, not waited for, where the session can - TODO 721
                barch::tx_lock_busy busy;
                const bool parks = lock_parks();
                const auto why = barch::tx_lock(*tx_state, name, as_sent, parks ? &busy : nullptr);
                if (!why.empty()) {
                    errors.emplace_back(why + ": ABORTTX or COMMITTX ends it");
                    return -1;
                }
                if (busy) {
                    tx_running.reset();     // or leaving call() would clear what parking lists
                    return lock_busy_met(std::move(as_sent), f, std::move(busy), name, true);
                }
            }
        }
        // a read command sees a transaction whole or not at all (key_space::tx_gate) - TODO 693
        bool gate_reads = false;
        /*
         * FLUSHALL and CLEARALL clear every space - TODO 663, 680. In a cluster each
         * replicated space is cleared through its own group (CLEARALL in
         * keyspace_api.cpp). A function run from the log reaches only its own space,
         * so it can't run them.
         */
        if (barch::functions::running_from_log() && params.size() >= 1) {
            std::string name = convert(params[0]);
            if (const auto colon = name.find_last_of(':'); colon != std::string::npos && colon + 1 < name.size())
                name = name.substr(colon + 1);
            for (auto& c : name) c = (char) std::toupper((unsigned char) c);
            if (barch::raft_writes::reaches_every_space(name)) {
                errors.emplace_back("ERR " + name + " clears every space, and a function run from the log"
                                    " reaches only its own");
                return -1;
            }
        }
        // KSPACE names the space it changes, which decides where it runs - TODO 670
        barch::key_space_ptr gather_into{};
        if (!raft_local_reads && !barch::functions::running_from_log() && barch::cluster::any_bound()
            && name_is(params[0], "KSPACE")) {
            int routed = 0;
            if (route_kspace(params, routed, gather_into))
                return routed;
        }
        // a run from the log is already in it: what it calls runs here, on this
        // member, like the rest of the run - TODO 673
        /*
         * Some commands go by spaces they name, not the connection's
         * (raft_writes::routes_itself): QUEUE by the queue's space, which its leader
         * takes (TODO 682), FUNCTIONS SYNC by its repositories' spaces, each written
         * where this node leads it (TODO 683), and REPLAPPLY by each record's space,
         * whose leader it goes through (TODO 684).
         */
        if (ks && ks->has_raft() && !raft_local_reads && !barch::functions::running_from_log()
            && !barch::raft_writes::routes_itself(upper_name(params[0]))) {
            /*
             * A space replicated with Raft - TODO 610, 613, 615. Writes go to the
             * leader of their shard's group: a space in one group is refused here,
             * and every write is refused at its shard when this node doesn't lead
             * its group. Reads go to the leader too, unless this connection asked
             * for follower reads: then a follower answers once it has applied
             * everything this connection has seen there, its own writes included.
             *
             * A space split over several groups can't tell from a command which
             * group a read lands in. A single-key read is checked where it takes
             * its shard's latch, against that shard's group; any other read has
             * to be readable in every group of the space before it starts.
             */
            std::string name = convert(params[0]);
            // the command after a `space:` prefix, the way the session finds it - TODO
            // 660. Looked up as sent, `orders:GET` was no command, so it was no read,
            // and a follower answered it from its own copy
            if (const auto colon = name.find_last_of(':'); colon != std::string::npos && colon + 1 < name.size())
                name = name.substr(colon + 1);
            const std::string as_sent = name;
            for (auto& c : name) c = (char) std::toupper((unsigned char) c);
            auto fi = barch::barch_functions->find(name);
            /*
             * A stored function, by its name or through CALLF - TODO 672. It isn't a
             * builtin, so nothing here used to see it: it ran on whatever node it was
             * sent to, read that node's copy without the read rules, and on a follower
             * had its writes refused one by one. Now it's a data command like any
             * other: one that may write goes to the leader, and one whose header says
             * "write": false is a read.
             */
            bool stored = false;
            auto how = barch::functions::replication::read_only;
            if (fi == barch::barch_functions->end() || as_sent.find('.') != std::string::npos) {
                stored = true;
                how = barch::functions::replication_of(ks, as_sent);
            } else if (name == "CALLF" && params.size() > 1) {
                stored = true;
                how = barch::functions::replication_of(ks, convert(params[1]));
            }
            const bool stored_writes = stored && how != barch::functions::replication::read_only;
            gate_reads = stored ? !stored_writes
                                : (fi != barch::barch_functions->end() && !fi->second.is_write());
            /*
             * A function that may write, run from the log by every member - TODO 673:
             * it lands whole, in log order, so a read and a write in it can't be split
             * by another call. One that says "replicate": "effects" runs here instead,
             * on the leader, and its writes are replicated one by one.
             */
            /*
             * A space split over several groups - TODO 689: an entry in the first group's
             * log wrote the other groups' shards outside their logs, at a different
             * point on each member. So it runs here instead, its writes gathered, and
             * lands as one batch across the groups (TODO 666).
             */
            const bool split_space = ks->raft_groups().size() > 1;
            if (stored && how == barch::functions::replication::log) {
                if (!split_space)
                    return call_through_log(params);
                gather_into = ks;
            }
            /*
             * A write that isn't listed as replicated or server changes this node's
             * copy only, so a replicated space refuses it - TODO 663. ROLLBACK undid
             * writes the other members kept, KSPACE MERGE and LOAD wrote what no other
             * member had. See raft_writes.h.
             */
            // changes the server or the connection, not the space: no leader needed -
            // TODO 685 (START, STOP and FOREIGN FAKE's reads are data writes by category)
            bool server_class = false;
            if (!stored && name != "KSPACE" && fi != barch::barch_functions->end() && fi->second.is_write()) {
                auto upper = [&](size_t i) {
                    std::string a = params.size() > i ? convert(params[i]) : std::string();
                    for (auto& c : a) c = (char) std::toupper((unsigned char) c);
                    return a;
                };
                const auto kind = barch::raft_writes::of(name, upper(1), upper(2));
                if (kind == barch::raft_writes::kind::local) {
                    errors.emplace_back("ERR " + name + " changes this node's copy only, so a space replicated"
                                        " with Raft refuses it");
                    return -1;
                }
                if (kind == barch::raft_writes::kind::logged && split_space) {
                    // a function's install writes its key: gathered as above - TODO 689
                    if (name == "SETF" || name == "REMF") {
                        gather_into = ks;
                    } else if (barch::raft_writes::needs_one_point(name)) {
                        /*
                         * In every group's log, in turn, and each group's run acts on
                         * its own shards at its own point - TODO 690. The first error
                         * is the answer; one sent again finishes what's left (BEGIN
                         * leaves a group already in a transaction as it is).
                         */
                        int code = 0;
                        for (const auto& g : ks->raft_groups()) {
                            code = call_through_log(params, ks, g);
                            if (code != 0 || !errors.empty())
                                return code != 0 ? code : -1;
                        }
                        return code;
                    } else {
                        return call_through_log(params);
                    }
                } else if (kind == barch::raft_writes::kind::logged) {
                    return call_through_log(params);
                }
                server_class = kind == barch::raft_writes::kind::server;
            }
            /*
             * Not a command a transaction runs itself - TODO 695: its reads are checked at the
             * prepare instead, and its writes go through this as plain SETs and DELs.
             */
            if (!server_class && !tx_layer
                && (stored || (fi != barch::barch_functions->end() && fi->second.is_data()))) {
                const auto groups = ks->raft_groups();
                barch::cluster::clear_committed();
                if (stored ? stored_writes : fi->second.is_write()) {
                    /*
                     * Not COMMITTX, whose prepare and commit go to each group's leader
                     * themselves, as they do from any node in a split space: a transaction
                     * run on a follower reads that follower's snapshot - TODO 701. Nor a
                     * function joining a transaction, whose writes go to the overlay as a
                     * SET in it does, and reach the leader with COMMITTX.
                     */
                    if (groups.size() == 1 && !groups[0]->leader() && name != "COMMITTX" && !tx_function) {
                        errors.emplace_back(groups[0]->not_leader());
                        return -1;
                    }
                } else if (!groups.empty()) {
                    raft_reading = true;
                    barch::cluster::begin_reads(raft_session.get());
                    if (groups.size() == 1 || stored || !single_key_read(name)) {
                        try {
                            for (const auto& g : groups)
                                barch::cluster::check_read(g);
                        } catch (const std::exception& e) {
                            barch::cluster::end_reads();
                            errors.emplace_back(e.what());
                            return -1;
                        }
                    }
                }
            }
        }
        /*
         * A space with no Raft, once a transaction has begun in it - TODO 450: a read
         * command holds the gate too, so it sees a local commit whole or not at all.
         * One that never has a transaction doesn't pay for it.
         */
        /*
         * Not while this thread builds a space either - TODO 714. That holds the keyspace
         * registry lock, and a gated read that looks a space up (SPACES, say) takes them
         * the other way round. TSan found it: a USE making a new space, its AUTH and
         * configuration reads waiting on a gate, against a read under the gate waiting
         * on the registry. What a space reads as it's built is its configuration.
         */
        if (!gate_reads && !tx_layer && ks && !ks->has_raft() && ks->tx_used.load(std::memory_order_relaxed)
            && !barch::building_space()) {
            const auto name = upper_name(params[0]);
            auto fi = barch::barch_functions->find(name);
            /*
             * Not CALLF, which is a read by category but runs a script, and a script may
             * commit a transaction of its own: COMMITTX takes this gate alone, on the
             * same thread, while CALLF held it shared - TODO 705. A script's reads don't
             * take the gate in any case (TODO 698).
             *
             * Nor INFO, PING or CLIENT, which read no keys: INFO transactions is how a
             * stuck commit is found, and waiting on that commit's gate it couldn't be -
             * TODO 720.
             */
            const auto& cats = get_category_map();
            gate_reads = fi != barch::barch_functions->end() && !fi->second.is_write() && name != "CALLF"
                         && !fi->second.cats[cats.at("stats")] && !fi->second.cats[cats.at("connection")];
        }
        /*
         * A client's write, in a space that has had a pessimistic transaction - TODO 719.
         * Its shards refuse it a key another transaction has locked; a string write waits
         * here first, holding nothing, for the lock to go. Not a transaction's own
         * commands, which write its overlay, nor a run from the log or a replica's apply.
         */
        std::optional<barch::tx_locks::guard_scope> lock_checked;
        if (!tx_layer && !tx_function && ks && ks->has_locks.load(std::memory_order_acquire)
            && !barch::functions::running_from_log()) {
            const auto name = upper_name(params[0]);
            auto fi = barch::barch_functions->find(name);
            // a stored function's barch.store writes are checked at the shards, as they go
            const bool stored = fi == barch::barch_functions->end() || name == "CALLF";
            if (stored)
                lock_checked.emplace(true);
            else if (fi->second.is_write() && name != "REPLAPPLY" && name != "COMMITTX" && name != "ABORTTX"
                     && name != "BEGINTX") {
                lock_checked.emplace(true);
                std::vector<std::string> as_sent;
                as_sent.reserve(params.size());
                for (const auto& p : params)
                    as_sent.push_back(convert(p));
                barch::tx_lock_busy busy;
                const bool parks = lock_parks();
                const auto why = barch::tx_plain_wait(ks, name, as_sent, parks ? &busy : nullptr);
                if (!why.empty()) {
                    errors.emplace_back(why);
                    return -1;
                }
                if (busy)
                    return lock_busy_met(std::move(as_sent), f, std::move(busy), name, false);
            }
        }
        auto cr = call_route(params);
        if (cr.net_error == 0) {
            return cr.call_error;
        }
        if (host != nullptr) {
            if (host->call(results, params).ok()) {
                return 0;
            }else {
                return -1;
            }
        }
        for (const auto& s : params) {
            args.push_back(s);
        }
        try {
            if (gather_into) {
                /*
                 * A KSPACE MERGE into a replicated space - TODO 670: its writes are
                 * gathered as it makes them, and go to the target's group as one
                 * batch, so every member gets the whole merge or none of it.
                 */
                std::vector<barch::cluster::batched> got;
                {
                    barch::cluster::batching gathering(got);
                    cr.call_error = f(*this, args);
                }
                std::string why;
                if (cr.call_error == 0 && errors.empty() && !barch::cluster::commit_gathered(got, why)) {
                    results.clear();
                    errors.emplace_back(why.empty() ? std::string("ERR the merge wasn't committed") : why);
                    cr.call_error = -1;
                }
            } else if (tx_layer) {
                // on the overlay, with its reads logged; the connection's space after - TODO 701
                struct on_overlay {
                    barch::key_space_ptr& ks;
                    barch::key_space_ptr was;
                    on_overlay(barch::key_space_ptr& ks, const barch::key_space_ptr& overlay) : ks(ks), was(ks) {
                        ks = overlay;
                    }
                    ~on_overlay() { ks = was; }
                } swapped{ks, tx_state->overlay};
                barch::tx_reading_scope reading(tx_state->reads.get());
                barch::tx_active_scope running;
                cr.call_error = f(*this, args);
            } else if (gate_reads) {
                std::shared_lock reading(ks->tx_gate);
                /*
                 * A transaction over several groups committed in some of them here and
                 * not yet the rest - TODO 694: wait for the rest, without the gate, which
                 * their commits need. Ten seconds is past a roll forward's five.
                 */
                if (ks->tx_partial.load() > 0) {
                    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(10);
                    while (ks->tx_partial.load() > 0) {
                        reading.unlock();
                        {
                            std::unique_lock w(ks->tx_settle_lock);
                            ks->tx_settled.wait_until(w, until, [&] { return ks->tx_partial.load() <= 0; });
                        }
                        reading.lock();
                        if (ks->tx_partial.load() > 0 && std::chrono::steady_clock::now() >= until)
                            break;
                    }
                }
                if (ks->tx_partial.load() > 0) {
                    errors.emplace_back("TRYAGAIN a transaction is landing in this space's groups");
                    cr.call_error = -1;
                } else {
                    cr.call_error = f(*this, args);
                }
            } else {
                cr.call_error = f(*this, args);
            }
        }catch (const std::exception& e) {
            ++statistics::exceptions_raised;
            errors.emplace_back(e.what());
            cr.call_error = -1;
        }
        /*
         * Past tx_max_keys or tx_max_bytes now - TODO 701: that's the answer rather than
         * the command's, and the transaction can only be ended. What the command did is
         * in the overlay, which never commits. A GET that wrote its value straight to
         * the reply has answered already, so its read stands and the next command says.
         */
        if ((tx_layer || tx_function) && tx_state->open && !barch::tx_after(*tx_state).empty() && !reply_sent) {
            results.clear();
            errors.clear();
            errors.emplace_back(tx_state->failed + ": ABORTTX or COMMITTX ends it");
            cr.call_error = -1;
        }
        if (ks && ks->has_raft() && !raft_local_reads && !barch::functions::running_from_log()) {
            // what this command saw becomes what the connection has seen - TODO 613
            for (const auto& [group, at] : barch::cluster::committed()) {
                auto& seen = raft_session->after[group];
                if (at > seen) seen = at;
            }
            barch::cluster::clear_committed();
            if (raft_reading)
                barch::cluster::end_reads();
        }
        if (cr.call_error != 0) {
            if (errors.empty())
                errors.emplace_back("call failed");
            return cr.call_error;
        }
        if (!errors.empty()) {
            cr.call_error = -1;
        }
        return cr.call_error;
    }
    /**
     * A command every member of the space's group runs, from the log - TODO 669. It
     * goes in the log as it came; each member, this one too, runs it as it commits,
     * and what it answered here is the reply. While it commits, every shard of the
     * space counts it as committing (abstract_shard::command_begin), since its run
     * may need their latches. A space split over several groups logs it in the
     * first, and the members of that group run it.
     */
    /**
     * KSPACE in a cluster - TODO 670. Finds the space the subcommand changes. When
     * that one is replicated: DEPENDS, RELEASE, DROP and OPTION SET go to its log (`routed` is
     * the answer, and true comes back); MERGE runs here, on its leader, with
     * `gather_into` set so the call gathers its writes. Anything else, or a space
     * that isn't replicated, runs here as it always did.
     */
    template<typename VT>
    bool route_kspace(const VT& params, int& routed, barch::key_space_ptr& gather_into) {
        std::vector<std::string> words;
        words.reserve(params.size());
        for (const auto& p : params)
            words.push_back(convert(p));
        arg_t a;
        for (const auto& w : words)
            a.push_back(art::value_type{w.data(), w.size()});
        art::kspace_spec parser(a);
        if (parser.parse_options() != 0)
            return false;                   // the command answers its own syntax error
        auto named = [](const std::string& n) -> barch::key_space_ptr {
            return !n.empty() && barch::keyspace_exists(n) ? barch::get_keyspace(n) : nullptr;
        };
        barch::key_space_ptr target;
        bool merge = false;
        if (parser.is_depends || parser.is_release) {
            target = named(parser.dependant);
        } else if (parser.is_drop) {
            target = parser.source.empty() ? ks : named(parser.source);
        } else if (parser.is_merge) {
            merge = true;
            target = parser.is_merge_default ? (ks ? ks->source() : nullptr) : named(parser.source);
        } else if (parser.is_option && parser.is_set) {
            target = ks;                    // the connection's space - TODO 669
        } else {
            return false;
        }
        if (!target || !target->has_raft())
            return false;
        if (!merge) {
            routed = call_through_log(params, target);
            return true;
        }
        const auto groups = target->raft_groups();
        if (groups.size() == 1 && !groups[0]->leader()) {
            errors.emplace_back(groups[0]->not_leader());
            routed = -1;
            return true;
        }
        gather_into = target;
        return false;
    }

    template<typename VT>
    int call_through_log(const VT& params, barch::key_space_ptr target = nullptr,
                         barch::cluster::binding_ptr via = nullptr) {
        // the space the command changes, which KSPACE names - TODO 670
        if (!target)
            target = ks;
        const auto groups = target->raft_groups();
        if (groups.empty()) {
            errors.emplace_back("TRYAGAIN this space's raft group isn't bound here yet");
            return -1;
        }
        /*
         * Who it runs as goes with it - TODO 667: the user, and its rights in this
         * space as this node worked them out. Users are each node's own, so every
         * member takes these rather than its own idea of the user.
         */
        std::vector<std::string> argv;
        argv.reserve(params.size() + 2);
        argv.push_back(user);
        std::string rights;
        for (bool r : (target == ks ? get_space_acl() : acl_for(target->get_canonical_name())))
            rights.push_back(r ? '1' : '0');
        argv.push_back(std::move(rights));
        for (const auto& p : params)
            argv.push_back(convert(p));
        barch::cluster::clear_committed();
        barch::cluster::reply_ptr reply;
        std::string why;
        auto result = barch::cluster::binding::result::refused;
        const auto shards = target->get_shards();
        size_t held = 0;
        try {
            for (; held < shards.size(); ++held)
                shards[held]->command_begin();
            result = (via ? via : groups[0])->commit_command(target->get_canonical_name(), argv, reply, why);
        } catch (const std::exception& e) {
            why = e.what();
        }
        for (size_t i = 0; i < held; ++i)
            shards[i]->command_end();
        for (const auto& [group, at] : barch::cluster::committed()) {
            auto& seen = raft_session->after[group];
            if (at > seen) seen = at;
        }
        barch::cluster::clear_committed();
        if (result != barch::cluster::binding::result::committed) {
            errors.emplace_back(why.empty() ? std::string("ERR the command wasn't committed") : why);
            return -1;
        }
        const auto* r = dynamic_cast<const rpc_reply*>(reply.get());
        if (!r && reply) {
            // run by the leader of a group led elsewhere (TODO 690): its OK or its error
            if (auto e = reply->error(); !e.empty()) {
                errors.emplace_back(std::move(e));
                return -1;
            }
            results.clear();
            results.emplace_back(std::string("OK"));
            return 0;
        }
        if (!r) {
            errors.emplace_back("ERR the command ran on every member but its reply here was lost");
            return -1;
        }
        results = r->results;
        errors = r->errors;
        return r->code;
    }
    template<typename TC, typename VT>
    Variable callv(const VT& params, TC&& f, Variable def = nullptr) {
        return retval(call(params, f),def);
    }
    // write_result treats r >= 0 as results. An empty results vector is a RESP
    // null, so a continue that only push_error'd has to return -1 here.
    int call_blocks() {
        results.clear();
        errors.clear();
        args.clear();
        block_retry = false;
        block_fun(*this, blocks);
        return errors.empty() ? 0 : -1;
    }
    [[nodiscard]] int script_depth() const override {
        return nest_depth;
    }
    void set_script_depth(int d) override {
        nest_depth = d;
    }
    std::string get_info() const override {
        if (!info_fun) return "";
        return info_fun();
    }
    [[nodiscard]] const std::string& get_user() const override  {
        return user;
    }
    [[nodiscard]] const heap::vector<bool>& get_acl() const override {
        return acl;
    }
    void set_acl(const std::string& user,const heap::vector<bool>& acl) override {
        this->user = user;
        this->acl = acl;
        this->space_acl = barch::read_space_overrides(user);
        // built from the rights that just changed
        this->script_ifaces.clear();
        this->resolved.clear();
        // whatever was worked out for the old rights is stale
        this->acl_space = "\x01none";
    };

    heap::string_map<caller::resolved>* resolutions(const std::string& space) override {
        if (resolved_space != space) {
            resolved.clear();          // a different space resolves differently
            resolved_space = space;
        }
        return &resolved;
    }

    barch::foreign::call_interface_ptr& script_interface(const std::string& running_in,
                                                        const std::string& defined_in) override {
        for (size_t i = 0; i < script_ifaces.size(); ++i) {
            auto& p = script_ifaces[i];
            if (p && p->running_in == running_in && p->defined_in == defined_in) {
                if (i)
                    std::rotate(script_ifaces.begin(), script_ifaces.begin() + (long) i,
                                script_ifaces.begin() + (long) i + 1);
                return script_ifaces.front();
            }
        }
        if (script_ifaces.size() >= max_script_ifaces)
            script_ifaces.pop_back();
        script_ifaces.insert(script_ifaces.begin(), nullptr);
        return script_ifaces.front();
    }

    [[nodiscard]] const heap::vector<bool>& acl_for(const std::string& space) override {
        if (space_acl.find(space) == space_acl.end() && !barch::server_written(space))
            return acl;
        // not cached: this is the cross-space path, which is rarer than the one the
        // connection is actually working in
        named_acl = barch::rights_in(space, acl, space_acl);
        return named_acl;
    }

    [[nodiscard]] const heap::vector<bool>& get_space_acl() override {
        // no overrides and not a space only the server writes: the global rights,
        // as before - TODO 594
        if (space_acl.empty() && !(ks && barch::server_written(ks->get_canonical_name())))
            return acl;
        const std::string& in = ks ? ks->get_canonical_name() : acl_space;
        if (in != acl_space) {
            effective_acl = barch::rights_in(in, acl, space_acl);
            acl_space = in;
        }
        return effective_acl;
    }
    barch::key_space_ptr& kspace() override {

        if (!ks ) {
            throw_exception<std::runtime_error>("key space not set");
        }
        return ks;
    }
    barch::key_space_ref ks_ref() override {
        if (!ks ) {
            throw_exception<std::runtime_error>("key space not set");
        }
        return ks.get();
    }

    void set_kspace(const barch::key_space_ptr& kspace) override{
        if (ks != kspace) {
            this->ks = kspace;
            update_routes();
        }
    }
    void use(const std::string& name) override {
        set_kspace( barch::get_keyspace(name));
    }
    void add_block(const keys_t &key_names, uint64_t to_ms,  std::function<void(caller&, const keys_t&)> fn) override {
        this->blocks = key_names;
        this->block_to_ms = to_ms;
        this->block_fun = fn;
        for (auto& d:blocks) {
            d.space = ks;
        }
    }
    void set_blocks_registered(std::function<void()> fn) override {
        blocks_registered_fun = std::move(fn);
    }

    void after_blocks_registered() override {
        if (blocks_registered_fun) {
            auto fn = blocks_registered_fun;
            blocks_registered_fun = nullptr;
            fn();
        }
    }

    void transfer_rpc_blocks(const barch::abstract_session_ptr& session) override {
        if (!counted_blocked && !blocks.empty()) {
            counted_blocked = true;
            ++statistics::blocked_clients;
        }
        for (auto &d: blocks) {
            auto shard = d.shard();
            std::unique_lock lck(shard->get_latch());
            d.space->get(d.shard_index)->add_rpc_block(d.key, session);
        }
        //add_rpc_blocks(caller.get_blocks(),session);
    }
    void erase_blocks(const barch::abstract_session_ptr& session) override {
        if (counted_blocked) {
            counted_blocked = false;
            --statistics::blocked_clients;
        }
        for (auto &d: blocks) {
            auto shard = d.shard();
            std::unique_lock lck(shard->get_latch());
            shard->erase_rpc_block(d.key, session);
        }
        clear_blocks();
    }
    bool has_blocks() override {
        return !this->blocks.empty();
    }
    bool block_retry = false;
    void retry_block() override {
        block_retry = true;
    }
    bool take_block_retry() override {
        bool r = block_retry;
        block_retry = false;
        return r;
    }
    /**
     * take over the blocks another caller registered, and leave it with none.
     *
     * An asynchronous call runs against a copy of the session's caller, so a blocking
     * command that lands in an asynchronous batch registers its blocks on that copy -
     * which is thrown away with the context, leaving the command not blocking at all.
     * They have to come back to the caller the session works with, because that is the
     * one that answers when the block resolves.
     *
     * The key spaces already stamped on them are kept rather than restamped: they
     * belong to the caller that made the blocks, which may have been on another space.
     */
    void adopt_blocks(rpc_caller& from) {
        this->blocks = from.blocks;
        this->block_to_ms = from.block_to_ms;
        this->block_fun = from.block_fun;
        // a key lock's re-check once the wait is registered - TODO 721
        this->blocks_registered_fun = std::move(from.blocks_registered_fun);
        from.blocks_registered_fun = nullptr;
        from.clear_blocks();
    }
    void clear_blocks() {
        this->blocks.clear();
    }
    [[nodiscard]] auto& get_blocks() const {
        return this->blocks;
    }

    void start_call_buffer() override {
        if (!call_buffering) {
            commands.clear();
        }
        call_buffering = true;
    }


    /**
     * A key lock someone else holds, met where the session parks - TODO 721. A first
     * try parks: the session waits on the key's shard under its wake name, with the
     * time tx_lock_wait_ms leaves, and its I/O thread goes on. A retry says so and goes
     * back to the parked one, which parks again or answers the timeout.
     *
     * Nothing of the command has run: its locks are taken before it does, so running it
     * again from the top when it's woken is the same command. Locks it got before the
     * busy one are kept, and taking them again is nothing new.
     */
    template<typename TC>
    int lock_busy_met(std::vector<std::string> as_sent, TC&& f, barch::tx_lock_busy busy, const std::string& name,
                      bool in_tx) {
        if (lock_mode == lock_wait::retry) {
            lock_busy = std::move(busy);
            lock_busy_in_tx = in_tx;
            return 0;
        }
        const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(barch::get_tx_lock_wait_ms());
        call_type again = f;
        park_on_lock(std::make_shared<parked_lock>(parked_lock{std::move(as_sent), std::move(again), ks, until,
                                                               in_tx, name}),
                     busy);
        return 0;
    }
    struct parked_lock {
        std::vector<std::string> args;
        call_type f;
        barch::key_space_ptr space;
        std::chrono::steady_clock::time_point until;
        bool in_tx;
        std::string name;
        std::string key;        // the lock it's parked on now
    };
    void park_on_lock(const std::shared_ptr<parked_lock>& p, const barch::tx_lock_busy& busy) {
        p->key = busy.key;
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                p->until - std::chrono::steady_clock::now()).count();
        keys_t keys;
        keys.emplace_back(barch::tx_lock_wake_key(busy.key),
                          p->space->get_shard_index(art::value_type(busy.key.data(), busy.key.size())));
        // at least 1: no time at all is no timeout to the session
        add_block(keys, (uint64_t) std::max<int64_t>(1, left), [p](caller& c, const keys_t& woken) {
            static_cast<rpc_caller&>(c).lock_woken(p, woken.empty());
        });
        for (auto& d : blocks)
            d.space = p->space;
        const std::string owner = p->in_tx ? tx_state->id : std::string();
        set_blocks_registered([space = p->space, owner, key = busy.key]() {
            barch::tx_lock_recheck(space, owner, key);
        });
        if (p->in_tx)
            barch::tx_lock_waiting(tx_state->status.get(), p->name, &busy);
    }
    /** a parked command woken, by a lock let go of or by its time running out */
    void lock_woken(const std::shared_ptr<parked_lock>& p, bool timed_out) {
        if (p->in_tx)
            barch::tx_lock_waiting(tx_state->status.get(), p->name, nullptr);
        // in the space it was sent to, `space:CMD` included
        const auto was = ks;
        set_kspace(p->space);
        lock_mode = lock_wait::retry;
        lock_busy = {};
        call(p->args, p->f);
        lock_mode = lock_wait::park;
        set_kspace(was);
        if (!lock_busy)
            return;                 // it ran: its answer is in results
        results.clear();
        errors.clear();
        auto busy = std::move(lock_busy);
        lock_busy = {};
        if (timed_out || std::chrono::steady_clock::now() >= p->until) {
            errors.emplace_back(p->in_tx ? barch::tx_lock_timed_out(*tx_state, busy) + ": ABORTTX or COMMITTX ends it"
                                         : barch::tx_plain_timed_out(busy));
            return;
        }
        /*
         * Someone else got in first: parked again, on whichever key is held now, with the
         * time that's left. The wake took it off the key's list, and the session puts it
         * back on (retry_block).
         */
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                p->until - std::chrono::steady_clock::now()).count();
        /*
         * Got past the key it was woken for, and now waits on another: the wake it took
         * would have gone on to the next waiter on that key had it finished, and that
         * one may be able to go on now (a reader behind a reader, say). Waking it is
         * what passing the turn would have done - TODO 728. Not when it waits on the
         * same key again: the next one would only find it held too.
         */
        if (busy.key != p->key) {
            barch::tx_lock_wake(p->space, {p->key});
            p->key = busy.key;
        }
        block_data d(barch::tx_lock_wake_key(busy.key),
                     p->space->get_shard_index(art::value_type(busy.key.data(), busy.key.size())));
        d.space = p->space;
        if (blocks.empty())
            blocks.push_back(d);
        else
            blocks[0] = d;          // the vector stays: its owner is reading it
        block_to_ms = (uint64_t) std::max<int64_t>(1, left);
        const std::string owner = p->in_tx ? tx_state->id : std::string();
        set_blocks_registered([space = p->space, owner, key = busy.key]() {
            barch::tx_lock_recheck(space, owner, key);
        });
        if (p->in_tx)
            barch::tx_lock_waiting(tx_state->status.get(), p->name, &busy);
        retry_block();
    }

    /** each queued command run in turn, its reply in buffered_results */
    void run_queued() {
        buffered_results.clear();
        for (auto& cmd: commands) {
            this->ks = cmd.space;
            int e = this->call(cmd.args, cmd.call);
            // analyze results
            if (e != 0) {
                buffered_results.emplace_back(errors[0]);
            } else {
                if (results.empty()) {
                    buffered_results.emplace_back(nullptr);
                }else {
                    for (auto& r: results) {
                        buffered_results.emplace_back(r);
                    }
                }
            }
        }
    }

    /*
     * EXEC as one transaction - TODO 710. When every queued command is one a transaction
     * takes, all in one space with no `space:` prefix, and no transaction is open here,
     * EXEC runs them as one: BEGINTX, the commands on its overlay, COMMITTX. So another
     * client never reads half of it, and in a split Raft space it lands in every group
     * or none. The client saw nothing in between, so a TXCONFLICT runs the whole batch
     * again, up to `exec_tries` times.
     *
     * False when the batch isn't one, and EXEC runs it the old way: one command after
     * another. That's any other command (redis-py's pipeline wraps all sorts in
     * MULTI/EXEC), a space a transaction can't overlay, and a MULTI inside an open
     * BEGINTX, whose commands already run in that transaction. True when it ran, with
     * `failed` set if it couldn't commit.
     */
    static constexpr int exec_tries = 64;
    bool exec_as_transaction(std::string& failed) {
        if (tx_state->open || commands.empty())
            return false;
        const auto space = commands.front().space;
        if (!space)
            return false;
        for (const auto& cmd : commands) {
            if (cmd.space != space || cmd.args.empty() || cmd.args[0].find(':') != std::string::npos
                || !barch::tx_takes(upper_name(cmd.args[0])))
                return false;
        }
        const auto begin = barch::barch_functions->find("BEGINTX");
        const auto commit = barch::barch_functions->find("COMMITTX");
        if (begin == barch::barch_functions->end() || commit == barch::barch_functions->end())
            return false;
        const std::vector<std::string> begin_args{"BEGINTX"}, commit_args{"COMMITTX"};
        for (int attempt = 1;; ++attempt) {
            ks = space;
            if (this->call(begin_args, begin->second.call) != 0 || !tx_state->open) {
                if (attempt == 1)
                    return false;       // nothing ran yet, so the old way is still open
                failed = errors.empty() ? std::string("ERR EXEC could not begin its transaction") : errors[0];
                return true;
            }
            run_queued();
            ks = space;
            if (this->call(commit_args, commit->second.call) == 0)
                return true;
            const std::string why = errors.empty() ? std::string("ERR EXEC could not commit") : errors[0];
            if (why.find("TXCONFLICT") == std::string::npos || attempt >= exec_tries) {
                failed = attempt >= exec_tries ? why + " (EXEC ran " + std::to_string(attempt) + " times)" : why;
                return true;
            }
            // under contention, a little apart, so the next try isn't beaten by the same writer
            if (attempt >= 4)
                std::this_thread::sleep_for(std::chrono::microseconds(50 * attempt));
        }
    }

    void finish_call_buffer() override {
        // every wake the queued commands raise waits until this scope ends, which is
        // what makes a transaction invisible to a blocked client. See DONE 125
        barch::defer_wakes hold_wakes;
        call_buffering = false;
        collecting_exec = true;
        buffered_results.clear();
        buffered_errors.clear();
        auto original = ks;
        std::string failed;
        if (!exec_as_transaction(failed))
            run_queued();
        if (!failed.empty()) {
            // it didn't commit, so none of its replies stand
            buffered_results.clear();
            results.clear();
            temp.clear();
            ks = original;
            commands.clear();
            collecting_exec = false;
            errors.clear();
            push_error(failed.c_str());
            return;
        }

        // EXEC answers one array of the queued replies, and it has to say so even when
        // there is one of them or none. The writer renders a results vector of one as
        // that value on its own and an empty one as a null, so `MULTI; PING; EXEC` used
        // to answer a bare +PONG where redis answers *1 +PONG. Closing it as an
        // aggregate keeps it an array of whatever length it is
        results.clear();
        temp.clear();
        start_array();
        for (auto& r: buffered_results) {
            temp.back().emplace_back(r);
        }
        end_array();
        buffered_results.clear();
        ks = original;
        commands.clear();
        collecting_exec = false;
    }
    void sort_pushed_results() override {
        std::sort(results.begin(), results.end());
    }
    bool is_buffering() const {
        return call_buffering;
    }
};

#endif //SWIG_CALLER_H
