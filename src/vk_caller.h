//
// Created by teejip on 5/18/25.
//

#ifndef VK_CALLER_H
#define VK_CALLER_H
#include "caller.h"
#include "valkeymodule.h"
#include "keys.h"
#include "module.h"
struct vk_caller : caller {
    ~vk_caller() override = default;
    ValkeyModuleCtx *ctx = nullptr;
    barch::key_space_ptr ks = get_default_ks();
    size_t call_counter{};
    std::string user{"default"};
    heap::vector<bool> acl{};
    [[nodiscard]] const std::string& get_user() const override  {
        return user;
    }
    [[nodiscard]] const heap::vector<bool>& get_acl() const override {
        return acl;
    }
    void set_acl(const std::string& user_,const heap::vector<bool>& acl_) override {
        this->user = user_;
        this->acl = acl_;
    };


    int null() override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithNull(ctx);
    }

    int boolean(bool val) override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithBool(ctx, val ? 1 : 0);
    }

    int vt(art::value_type v) override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithString(ctx, ValkeyModule_CreateString(ctx,v.chars(),v.size));
        //return ValkeyModule_ReplyWithStringBuffer(ctx, v.chars(), v.size);
    }

    int start_array() override {
        check_ctx();
        call_counter = 0;
        return ValkeyModule_ReplyWithArray(ctx,VALKEYMODULE_POSTPONED_LEN);
    }

    int end_array(size_t) override {
        check_ctx();
        ValkeyModule_ReplySetArrayLength(ctx, (long long)call_counter);
        return 0;
    }

    int long_long(int64_t l) override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithLongLong(ctx,l);
    };int any_int(int64_t l) override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithLongLong(ctx,l);
    };int any_int(uint64_t l) override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithLongLong(ctx,(long long)l);
    };
    int any_int(long long l) override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithLongLong(ctx,l);
    };
    int any_int(unsigned long long l) override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithLongLong(ctx,(long long)l);
    };

    int any_int(uint32_t l) override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithLongLong(ctx,l);
    };

    int any_int(int32_t l) override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithLongLong(ctx,l);
    };

    int double_(double l) override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithDouble(ctx,l);
    };

    int reply_values(const std::initializer_list<Variable>& keys) override {
        ++call_counter;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        for (auto &k : keys) {
            reply_variable(ctx, k);
        }
        ValkeyModule_ReplySetArrayLength(ctx, (long long)keys.size());
        return 0;
    }
    int reply(const std::string& value) override {
        reply_variable(ctx, {value});
        return 0;
    }

    int reply_encoded_key(art::value_type key) override {
        check_ctx();
        ++call_counter;
        return ::reply_encoded_key(ctx, key);
    }

    [[nodiscard]] int ok() override {
        return VALKEYMODULE_OK;
    }

    [[nodiscard]] int wrong_arity() override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_WrongArity(ctx);
    }

    [[nodiscard]] int syntax_error() override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithError(ctx,"syntax error");
    }

    [[nodiscard]] int error(const char * e)  override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithError(ctx,e);
    };

    void check_ctx() const {
        if (ctx == nullptr) {
            abort_with("invalid vk context");
        }
    }
    int simple(const char * v) override {
        check_ctx();
        ++call_counter;
        return ValkeyModule_ReplyWithSimpleString(ctx, v);
    };
    [[nodiscard]] int error() const override {
        return VALKEYMODULE_ERR;
    }
    int key_check_error(art::value_type k) override {
        check_ctx();
        ++call_counter;
        if (k.empty())
            return ValkeyModule_ReplyWithError(ctx, "No null keys");

        return ValkeyModule_ReplyWithError(ctx, "Unspecified key error");
    }

    template<typename TF>
    int vk_call(ValkeyModuleCtx *ctx_, ValkeyModuleString **argv, int argc, TF &&call) {
        this->ctx = ctx_;
        arg_t args {};
        args.resize(argc);
        ValkeyModule_AutoMemory(ctx);
        for (int i = 0; i < argc; i++) {
            size_t klen;
            const char *k = ValkeyModule_StringPtrLen(argv[i], &klen);
            args[i]= {k, klen};
        }
        int r = 0;
        try {
            r = call(*this,args);
        }catch (const std::exception& e) {
            r = error(e.what());
        }

        ctx = nullptr;
        return r;
    }
    barch::key_space_ptr &kspace() override {
        return ks;
    }
    void set_kspace(const barch::key_space_ptr& kspace) override{
        this->ks = kspace;
    }

    void use(const std::string& name) override {
        this->ks = barch::get_keyspace(name);
    }
};

#endif //VK_CALLER_H
