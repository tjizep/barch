//
// Created by teejip on 9/17/25.
//

#ifndef BARCH_BARCH_PARSER_H
#define BARCH_BARCH_PARSER_H
#include "vector_stream.h"
#include "barch_rpc.h"

namespace barch {
    class barch_parser {
    public:
        enum {
            header_size = 4
        };
        enum {
            barch_wait_for_header = 1,
            barch_wait_for_buffer_size,
            barch_wait_for_buffer,
            barch_parse_params
        };
        barch_parser& operator=(const barch_parser&) = delete;
        barch_parser(const barch_parser&) = delete;

        barch_parser() {
            caller.set_context(ctx_rpc);
        };

        ~barch_parser() = default;
        vector_stream in{};
        int state = barch_wait_for_header;
        uint32_t calls = 0;
        uint32_t c = 0;
        uint32_t buffers_size = 0;
        uint32_t replies_size = 0;

        rpc_caller caller{};
        heap::vector<uint8_t> replies{};
        heap::vector<uint8_t> buffer{};
        void clear() {
            state = barch_wait_for_header;
            calls = 0;
            buffers_size = 0;
            replies_size = 0;
            c = 0;
            replies.clear();
            buffer.clear();
            in.clear();
        }
        [[nodiscard]] size_t remaining() const {
            if (in.pos > in.buf.size()) {
                barch::std_err("invalid buffer size", in.buf.size());
                return 0;
            }
            return in.buf.size() - in.pos;
        }
        /**
         *
         * @return true if someting needs to be written
         */
        bool process(vector_stream& out) {
            while (remaining() > 0) {
                switch (state) {
                    case barch_wait_for_header:
                        if (remaining() >= header_size) {
                            readp(in,calls);
                            if (1 != calls) {
                                return false; // nothing to do - wait for more data
                            }
                            c = 0;
                            buffers_size = 0;
                            state = barch_wait_for_buffer_size;
                        }
                        break;
                    case barch_wait_for_buffer_size:
                        if (c < calls) {
                            if (remaining() < sizeof(buffers_size)) {
                                return false; // nothing to do - wait for more data
                            }
                            readp(in,buffers_size);
                            if (buffers_size == 0) {
                                barch::std_err("invalid buffer size", buffers_size);
                                clear();
                                return false;
                            }
                            if (buffer.size() < buffers_size) {
                                buffer.resize(buffers_size);
                            }
                            state = barch_wait_for_buffer;
                        }else {
                            state = barch_wait_for_header;
                            if (remaining()  >= in.buf.size())
                                return false;
                        }
                        break;
                    case barch_wait_for_buffer:{
                        // wait for input buffer to reach a target
                        if (remaining() < buffers_size) {
                            return false; // nothing to do - wait for more data
                        }
                        readp(in, buffer.data(), buffers_size);
                        int32_t r = 0;
                        if (buffers_size > rpc_max_param_buffer_size) {
                            r = -1;
                            replies.clear();
                            push_value(replies, error{"parameter buffer too large"});
                        }else {
                            // TODO: max buffer size check
                            auto bf = barch_functions; // take a snapshot
                            std::vector<std::string_view> params;
                            for (size_t i = 0; i < buffers_size;) {
                                auto vp = get_value(i, buffer);
                                params.emplace_back(vp.first.chars(), vp.first.size);
                                i = vp.second;
                            }
                            std::string cn = std::string{params[0]};
                            auto ic = bf->find(cn);
                            if (ic == bf->end()) {
                                barch::std_err("invalid call", cn);
                                writep(out, replies_size);
                                clear();
                                return true;
                            }
                            auto f = ic->second.call;
                            ++ic->second.calls;
                            r = caller.call(params,f);
                            replies.clear();
                            for (auto &v: caller.results) {
                                push_value(replies,v);
                            }
                            for (auto &v: caller.errors) {
                                push_value(replies,v);
                            }
                        }

                        replies_size = replies.size();
                        writep(out, r);
                        writep(out, replies_size);
                        writep(out, replies.data(), replies_size);
                        ++c;
                        if (c < calls) {
                            state = barch_wait_for_buffer;
                        }else {
                            state = barch_wait_for_header;
                            buffers_size = 0;
                        }

                        if (remaining() <= 0) {
                            in.clear();
                            return true;
                        }

                    }
                        break;
                    default:
                        barch::std_err("invalid state", state);
                        break;
                }
            }
            return false; // consumed all data but no action could be taken
        }
        void add_data(const uint8_t *data, size_t length) {
            in.buf.insert(in.buf.end(),data,data+length);
        }
    };

}
#endif //BARCH_BARCH_PARSER_H