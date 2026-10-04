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
#include <sys/stat.h>
#ifndef _WIN32
#include <sys/file.h>
#include <sys/sysmacros.h>
#endif
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
        // is_absolute rather than a leading '/', so C:/data counts on windows
        if (!path.empty() && std::filesystem::path(path).is_absolute())
            return path;
        const std::string& dir = data_dir();
        if (path.empty())
            return dir;
        return dir == "/" ? "/" + path : dir + "/" + path;
    }

#ifndef _WIN32
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
#endif

#ifdef _WIN32
    /*
     * No flock on windows, and stat has no inode to key on. A named mutex does the
     * same job: the name is the directory's canonical path, so the same directory
     * under two spellings is one claim, and windows drops it when the process ends
     * however it ends. Nothing is written into the directory.
     */
    bool hold_dir(const std::string& dir, std::string& err) {
        HANDLE h = CreateFileA(dir.c_str(), FILE_READ_ATTRIBUTES,
                               FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                               nullptr, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, nullptr);
        if (h == INVALID_HANDLE_VALUE) {
            err = "could not open " + dir + " to hold it (windows error "
                  + std::to_string(GetLastError()) + ")";
            return false;
        }
        char canonical[MAX_PATH * 4];
        const DWORD n = GetFinalPathNameByHandleA(h, canonical, sizeof(canonical),
                                                  FILE_NAME_NORMALIZED);
        CloseHandle(h);
        if (n == 0 || n >= sizeof(canonical)) {
            err = "could not resolve " + dir + " to hold it";
            return false;
        }
        std::string name = canonical;
        for (auto& c : name)
            c = (char) std::tolower((unsigned char) c);
        name = "Local\\barch-dir-" + std::to_string(std::hash<std::string>{}(name));

        static std::mutex mut;
        static auto* names = new std::map<std::string, HANDLE>;    // never destroyed
        std::lock_guard l(mut);
        if (names->count(name))
            return true;
        HANDLE m = CreateMutexA(nullptr, FALSE, name.c_str());
        if (m == nullptr) {
            barch::err({"could not lock", dir, ": windows error", (uint64_t) GetLastError(),
                        "- carrying on without, so nothing stops a second process using it"});
            return true;
        }
        if (GetLastError() == ERROR_ALREADY_EXISTS) {
            CloseHandle(m);
            err = dir + " is held by another barch process"
                  ". Two processes in one directory overwrite each other's files";
            return false;
        }
        (*names)[name] = m;
        return true;
    }
#else
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
#endif

    bool data_dir_held(std::string& err) {
        data_dir();             // pins, and so asks, if nothing has yet
        std::lock_guard l(pin().mut);
        if (!pin().held)
            err = pin().why;
        return pin().held;
    }
}
