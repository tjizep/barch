//
// Created by teejip on 11/9/25.
//
#include "asio_resp_session.h"

#include "barch_functions.h"
#include "keyspec.h"
#include "key_space.h"
#include "netstat.h"
#include "redis_parser.h"
#include "server.h"
#include <ctime>

barch::resp_session::resp_session(tcp::socket socket,char init_char)
              : socket_(std::move(socket)), timer( socket_.get_executor())
{
    parser.init(init_char);
    caller.info_fun = [this]() -> std::string {
        return get_info();
    };
    ++statistics::repl::redis_sessions;
}
barch::resp_session::~resp_session() {
    --statistics::repl::redis_sessions;
}

void barch::resp_session::start()
{
    do_read();
}
static bool is_authorized(const heap::vector<bool>& func,const heap::vector<bool>& user) {
    size_t s = std::min<size_t>(user.size(),func.size());
    if (s < func.size()) return false;
    for (size_t i = 0; i < s; ++i) {
        if (func[i] && !user[i])
            return false;
    }
    return true;
}
std::string barch::resp_session::get_info() const {
    auto rmote = socket_.remote_endpoint();
    auto lcal = socket_.local_endpoint();
    uint64_t seconds = (art::now() - created)/1000;
    std::string laddress = lcal.address().to_string()+":"+ std::to_string(lcal.port());
    std::string address = rmote.address().to_string()+":"+ std::to_string(rmote.port());

    std::string r =
        "id="+std::to_string(this->id)+" addr="+address+ " "
        "laddr="+laddress+" fd="+"10"+ " "
        "name="+""+" age="+std::to_string(seconds)+" "+
        "idle=0 flags=N capa= db=0 sub=0 psub=0 ssub=0 "+
        "multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=10 multi-mem=0 "+
        "rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem="+std::to_string(rpc_io_buffer_size+parser.get_max_buffer_size())+" "+
        "events=r cmd=client|info user="+caller.get_user()+" redir=-1 "+
        "resp=2 lib-name= lib-ver= "+
        "tot-net-in="+ std::to_string(bytes_recv)+ " " +
        "tot-net-out=" + std::to_string(bytes_sent)+ " " +
        "tot-cmds=" + std::to_string(calls_recv) + "\n";
    return r;
}
void barch::resp_session::write_result(int32_t r) {
    if (r < 0) {
        if (!caller.errors.empty())
            redis::rwrite(stream, error{caller.errors[0]});
        else
            redis::rwrite(stream, error{"null error"});
    } else {
        redis::rwrite(stream, caller.results);
    }
}

void barch::resp_session::run_params(vector_stream& stream, const std::vector<std::string_view>& params) {
    std::string cn{ params[0]};

    auto colon = cn.find_last_of(':');
    auto spc = caller.kspace();
    bool should_reset_space = false;
    try {
        if (colon != std::string::npos && colon < cn.size()-1) {
            std::string space = cn.substr(0,colon);
            cn = cn.substr(colon+1);

            if (!spc || spc->get_canonical_name() != space) {

                caller.set_kspace(barch::get_keyspace(space));
                should_reset_space = true;
            }
        }
        ++calls_recv;
        auto bf = barch_functions; // take a snapshot
        if (prev_cn != cn) {
            ic = bf->find(cn);
            prev_cn = cn;
            if (ic != bf->end() &&
                !is_authorized(ic->second.cats,caller.get_acl())) {
                redis::rwrite(stream, error{"not authorized"});
                return;
            }
        }
        if (ic == bf->end()) {
            redis::rwrite(stream, error{"unknown command"});
        } else {

            auto &f = ic->second.call;
            ++ic->second.calls;

            int32_t r = caller.call(params,f);
            if (!caller.has_blocks())
                write_result(r);
        }
    }catch (std::exception& e) {
        redis::rwrite(stream, error{e.what()});
    }
    if (should_reset_space)
        caller.set_kspace(spc); // return to old value
}
void barch::resp_session::do_read()
{
        auto self(shared_from_this());
        socket_.async_read_some(asio::buffer(data_, rpc_io_buffer_size),
            [this, self](std::error_code ec, std::size_t length)
        {

            if (!ec){
                bytes_recv += length;
                parser.add_data(data_, length);
                try {

                    stream.clear();
                    while (parser.remaining() > 0) {
                        auto &params = parser.read_new_request();
                        if (!params.empty()) {
                            run_params(stream, params);
                        }else {
                            break;
                        }
                    }
                    if (caller.has_blocks()) {
                        start_block_to();
                        add_caller_blocks();
                    }else {
                        do_write(stream);
                        do_read();
                    }
                }catch (std::exception& e) {
                    barch::std_err("error", e.what());
                }
            }
        });

}

void barch::resp_session::start_block_to() {
    if (caller.block_to_ms == 0 || caller.block_to_ms >= std::numeric_limits<long>::max()) {
        return;
    }
    timer.cancel();
    timer.expires_after(std::chrono::milliseconds(caller.block_to_ms));
    auto self(shared_from_this());
    timer.async_wait([this,self](const std::error_code& ec)
        {
            if (!ec) {
                do_block_to();
            }
        });
}
void barch::resp_session::add_caller_blocks() {
    caller.transfer_rpc_blocks(shared_from_this());
}
void barch::resp_session::erase_blocks() {
    caller.erase_blocks(shared_from_this());

}
void barch::resp_session::do_block_continue() {
    if (caller.has_blocks()) {
        auto self(shared_from_this());
        this->socket_.get_executor().execute([this,self]() {
            caller.call_blocks();
            write_result(0);
            erase_blocks();
            do_write(stream);
            do_read();

        });
    }
}
void barch::resp_session::do_block_to() {
    erase_blocks();
    caller.call_blocks();
    write_result(0);
    do_write(stream);
    do_read();
}
void barch::resp_session::do_write(vector_stream& stream) {
    auto self(shared_from_this());
    if (stream.empty()) return;

    asio::async_write(socket_, asio::buffer(stream.buf),
        [this, self](std::error_code ec, std::size_t length){
            if (!ec){
                    net_stat stat;
                    stream_write_ctr += length;
                    bytes_sent += length;
            }else {
                //art::std_err("error", ec.message(), ec.value());
            }
        });
}
