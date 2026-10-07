//
// What the rest of barch sees of clustering - TODO 610.
//
#include "cluster_hooks.h"

#include <atomic>
#include <map>
#include <mutex>

namespace barch::cluster {
    namespace {
        std::mutex& bindings_lock() {
            static auto* m = new std::mutex;
            return *m;
        }
        // never destroyed: shards hold on to what they were given, and may
        // outlive the statics at exit - TODO 57
        std::map<std::string, binding_ptr>& bindings() {
            static auto* b = new std::map<std::string, binding_ptr>;
            return *b;
        }
        std::atomic<runtime*> installed_runtime{nullptr};
    }

    binding_ptr binding_for(const std::string& canonical_space) {
        std::lock_guard l(bindings_lock());
        auto i = bindings().find(canonical_space);
        return i == bindings().end() ? nullptr : i->second;
    }

    void bind(const std::string& canonical_space, binding_ptr b) {
        std::lock_guard l(bindings_lock());
        bindings()[canonical_space] = std::move(b);
    }

    void unbind(const std::string& canonical_space) {
        std::lock_guard l(bindings_lock());
        bindings().erase(canonical_space);
    }

    void install(runtime* r) {
        installed_runtime.store(r);
    }

    bool start(std::string& err) {
        auto* r = installed_runtime.load();
        return r ? r->start(err) : true;
    }

    void stop() {
        if (auto* r = installed_runtime.load())
            r->stop();
    }

    bool built() {
        return installed_runtime.load() != nullptr;
    }

    bool command(const std::vector<std::string>& args, std::vector<std::string>& out, std::string& err) {
        auto* r = installed_runtime.load();
        if (!r) {
            err = "ERR this build has no cluster - configure it with -DBARCH_CLUSTER=ON";
            return false;
        }
        return r->command(args, out, err);
    }
}
