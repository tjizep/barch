#include "data_dir.h"
#include "lzr_log.h"

#include <filesystem>
#include <mutex>

namespace barch {
    namespace {
        // never destroyed: every save reads it, and a save can still be running on a
        // maintenance thread while the process exits - TODO 57's pattern
        struct pin_state {
            std::mutex mut;
            std::string dir;
            bool done = false;
        };
        pin_state& pin() {
            static auto* p = new pin_state;
            return *p;
        }
    }

    void pin_data_dir() {
        std::lock_guard l(pin().mut);
        if (pin().done)
            return;
        std::error_code ec;
        auto here = std::filesystem::current_path(ec);
        if (ec) {
            // nothing better to go on; relative names behave the way they always did
            barch::err({"could not read the working directory to keep data in:", ec.message(),
                        "- data files follow the working directory"});
            pin().dir = ".";
        } else {
            pin().dir = here.string();
            while (pin().dir.size() > 1 && pin().dir.back() == '/')
                pin().dir.pop_back();
        }
        pin().done = true;
        barch::log({"data files are kept in", pin().dir});
    }

    const std::string& data_dir() {
        {
            std::lock_guard l(pin().mut);
            if (pin().done)
                return pin().dir;
        }
        pin_data_dir();
        return pin().dir;
    }

    std::string data_path(const std::string& path) {
        if (!path.empty() && path.front() == '/')
            return path;
        const std::string& dir = data_dir();
        if (path.empty())
            return dir;
        return dir == "/" ? "/" + path : dir + "/" + path;
    }
}
