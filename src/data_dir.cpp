#include "data_dir.h"
#include "lzr_log.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <unistd.h>

namespace barch {
    namespace {
        // never destroyed: every save reads it, and a save can still be running on a
        // maintenance thread while the process exits - TODO 57's pattern
        struct pin_state {
            std::mutex mut;
            std::string dir;
            bool done = false;
            bool held = false;      // hold_dir got the directory - TODO 571
            std::string why;        // and why not, when it didn't
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
        pin().held = hold_dir(pin().dir, pin().why);
        if (!pin().held)
            barch::err({pin().why});
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

    namespace {
        // the directories this process holds, by device and inode, so the same one
        // under two names - the data directory and a change log kept in it - is one
        // claim. flock on a second descriptor would conflict with this process's own
        struct held_dirs {
            std::mutex mut;
            std::map<std::pair<dev_t, ino_t>, int> fds;   // kept open for the process's life
        };
        held_dirs& held() {
            static auto* h = new held_dirs;     // never destroyed, like pin_state
            return *h;
        }

        /** the pid holding a flock on this inode, from /proc/locks, or 0 */
        long flock_holder(const struct stat& st) {
            std::ifstream locks("/proc/locks");
            std::string line;
            while (std::getline(locks, line)) {
                std::istringstream in(line);
                std::string id, kind, advisory, mode, devino;
                long pid = 0;
                in >> id >> kind;
                if (kind == "->")
                    continue;               // someone waiting, not the holder
                in >> advisory >> mode >> pid >> devino;
                unsigned maj = 0, min = 0;
                unsigned long ino = 0;
                if (kind != "FLOCK"
                    || std::sscanf(devino.c_str(), "%x:%x:%lu", &maj, &min, &ino) != 3)
                    continue;
                if (maj == major(st.st_dev) && min == minor(st.st_dev) && ino == st.st_ino)
                    return pid;
            }
            return 0;
        }
    }

    bool hold_dir(const std::string& dir, std::string& err) {
        struct stat st{};
        if (::stat(dir.c_str(), &st) != 0) {
            err = "could not look at " + dir + " to hold it: " + std::strerror(errno);
            return false;
        }
        std::lock_guard l(held().mut);
        const auto key = std::make_pair(st.st_dev, st.st_ino);
        if (held().fds.count(key))
            return true;
        const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd < 0) {
            err = "could not open " + dir + " to hold it: " + std::strerror(errno);
            return false;
        }
        if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
            const int e = errno;
            ::close(fd);
            if (e != EWOULDBLOCK) {
                barch::err({"could not lock", dir, ":", std::strerror(e),
                            "- carrying on without, so nothing stops a second process using it"});
                return true;
            }
            const long pid = flock_holder(st);
            err = dir + " is held by another barch process"
                  + (pid > 0 ? " (pid " + std::to_string(pid) + ")" : std::string())
                  + ". Two processes in one directory overwrite each other's files";
            return false;
        }
        held().fds[key] = fd;
        return true;
    }

    bool data_dir_held(std::string& err) {
        data_dir();             // pins, and so asks, if nothing has yet
        std::lock_guard l(pin().mut);
        if (!pin().held)
            err = pin().why;
        return pin().held;
    }
}
