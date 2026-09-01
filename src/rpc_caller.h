//
// Created by teejip on 6/6/25.
//

#ifndef SWIG_CALLER_H
#define SWIG_CALLER_H
#include <cctype>
#include "caller.h"
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

struct rpc_caller : caller {
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
    /** see caller::script_interface */
    barch::foreign::call_interface_ptr script_iface{};
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

    // the session sets this to a blocking write on its socket. null means the
    // reply stays in results and is written after the call, as it always was.
    std::function<bool(const char*, size_t)> write_socket_bytes;
    // the session's reply buffer for this call. GET writes a bulk string
    // here from the leaf so the value is not copied into results first
    vector_stream* reply_out{nullptr};
    // true once write_socket* has put bytes on the socket. write_result must
    // not emit a second reply (an empty results vector is a RESP null).
    bool reply_sent{false};
    // EXEC collects inner replies as Variables. KEYS must not write the socket
    // then, or the items leave the EXEC array.
    bool collecting_exec{false};

    [[nodiscard]] const barch::foreign::function_states_ptr& function_states() const override {
        return luau_states;
    }

    [[nodiscard]] bool is_collecting_exec() const override {
        return collecting_exec;
    }

    [[nodiscard]] bool can_write_socket() const override {
        return (bool) write_socket_bytes && !call_buffering && !collecting_exec;
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

    bool write_socket(const Variable& v) override {
        if (!can_write_socket()) return false;
        vector_stream encoded;
        redis::rwrite(encoded, v, protocol);
        return write_encoded(encoded);
    }

    bool write_socket_array(size_t n) override {
        if (!can_write_socket()) return false;
        vector_stream encoded;
        redis::rwrite_header(encoded, '*', n);
        return write_encoded(encoded);
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
    int push_int(long long l) override {
        emplace_impl(l);
        return 0;
    }
    int push_int(unsigned long long l) override {
        emplace_impl(l);
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
    int set_int(size_t at, long long l) final {
       return set_impl(at, l);
    }
    int set_int(size_t at, unsigned long long l)  final {
        return set_impl(at, l);
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

            cr.call_error = f(*this, args);
        }catch (const std::exception& e) {
            ++statistics::exceptions_raised;
            errors.emplace_back(e.what());
            cr.call_error = -1;
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
        this->script_iface.reset();
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

    barch::foreign::call_interface_ptr& script_interface() override {
        return script_iface;
    }

    [[nodiscard]] const heap::vector<bool>& acl_for(const std::string& space) override {
        if (space_acl.empty())
            return acl;
        auto o = space_acl.find(space);
        if (o == space_acl.end())
            return acl;
        // not cached: this is the cross-space path, which is rarer than the one the
        // connection is actually working in
        named_acl = barch::apply_overrides(acl, o->second);
        return named_acl;
    }

    [[nodiscard]] const heap::vector<bool>& get_space_acl() override {
        if (space_acl.empty())
            return acl;                    // no overrides: the global rights, as before
        const std::string& in = ks ? ks->get_canonical_name() : acl_space;
        if (in != acl_space) {
            auto o = space_acl.find(in);
            effective_acl = (o == space_acl.end())
                ? acl                       // nothing said about this space
                : barch::apply_overrides(acl, o->second);
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


    void finish_call_buffer() override {
        // every wake the queued commands raise waits until this scope ends, which is
        // what makes a transaction invisible to a blocked client. See DONE 125
        barch::defer_wakes hold_wakes;
        call_buffering = false;
        collecting_exec = true;
        buffered_results.clear();
        buffered_errors.clear();
        auto original = ks;
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
