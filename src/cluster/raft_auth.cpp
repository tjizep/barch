//
// Signing Raft messages with the cluster's shared secret - TODO 620.
//
#include "raft_auth.h"

#include <openssl/core_names.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/params.h>

#include <memory>
#include <stdexcept>

namespace barch::cluster {
    namespace {
        // one HMAC over many pieces; numbers go in little endian, so two builds
        // that agree on the message agree on the bytes
        class hmac {
        public:
            explicit hmac(const std::string& key) {
                static EVP_MAC* mac = EVP_MAC_fetch(nullptr, "HMAC", nullptr);
                if (!mac) throw std::runtime_error("OpenSSL has no HMAC");
                ctx.reset(EVP_MAC_CTX_new(mac));
                char digest[] = "SHA256";
                OSSL_PARAM params[] = {
                    OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_DIGEST, digest, 0),
                    OSSL_PARAM_construct_end()
                };
                if (!ctx || !EVP_MAC_init(ctx.get(), (const unsigned char*) key.data(), key.size(), params))
                    throw std::runtime_error("could not start an HMAC");
            }
            void bytes(const void* p, size_t n) {
                if (n) EVP_MAC_update(ctx.get(), (const unsigned char*) p, n);
            }
            void text(const char* s) {
                bytes(s, std::char_traits<char>::length(s) + 1);
            }
            void u64(uint64_t v) {
                unsigned char b[8];
                for (int i = 0; i < 8; ++i) b[i] = (unsigned char) (v >> (8 * i));
                bytes(b, 8);
            }
            std::string done() {
                unsigned char out[EVP_MAX_MD_SIZE];
                size_t n = 0;
                if (!EVP_MAC_final(ctx.get(), out, &n, sizeof(out)))
                    throw std::runtime_error("could not finish an HMAC");
                return {(const char*) out, n};
            }

        private:
            struct free_ctx {
                void operator()(EVP_MAC_CTX* c) const { EVP_MAC_CTX_free(c); }
            };
            std::unique_ptr<EVP_MAC_CTX, free_ctx> ctx;
        };

        void header(hmac& h, nuraft::req_msg& req) {
            h.u64(req.get_term());
            h.u64((uint64_t) req.get_type());
            h.u64((uint64_t) (uint32_t) req.get_src());
            h.u64((uint64_t) (uint32_t) req.get_dst());
            h.u64(req.get_last_log_term());
            h.u64(req.get_last_log_idx());
            h.u64(req.get_commit_idx());
        }
    }

    std::string sign_request(const std::string& secret, nuraft::req_msg& req) {
        hmac h(secret);
        h.text("barch raft request 1");
        header(h, req);
        auto& entries = req.log_entries();
        h.u64(entries.size());
        for (auto& e : entries) {
            h.u64(e->get_term());
            h.u64((uint64_t) e->get_val_type());
            auto& b = e->get_buf();
            h.u64(b.size());
            h.bytes(b.data_begin(), b.size());
        }
        return h.done();
    }

    std::string sign_response(const std::string& secret, nuraft::req_msg& req, nuraft::resp_msg& resp) {
        hmac h(secret);
        h.text("barch raft response 1");
        header(h, req);
        h.u64(resp.get_term());
        h.u64((uint64_t) resp.get_type());
        h.u64((uint64_t) (uint32_t) resp.get_src());
        h.u64((uint64_t) (uint32_t) resp.get_dst());
        h.u64(resp.get_next_idx());
        h.u64(resp.get_accepted() ? 1 : 0);
        return h.done();
    }

    std::string sign_words(const std::string& secret, const char* what, const std::vector<std::string>& words) {
        hmac h(secret);
        h.text("barch words 1");
        h.text(what);
        h.u64(words.size());
        for (const auto& w : words) {
            h.u64(w.size());
            h.bytes(w.data(), w.size());
        }
        const auto raw = h.done();
        static const char* d = "0123456789abcdef";
        std::string out;
        for (unsigned char c : raw) {
            out.push_back(d[c >> 4]);
            out.push_back(d[c & 15]);
        }
        return out;
    }

    bool same_signature(const std::string& a, const std::string& b) {
        return a.size() == b.size() && !a.empty() && CRYPTO_memcmp(a.data(), b.data(), a.size()) == 0;
    }
}
