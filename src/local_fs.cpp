//
// See local_fs.h.
//
#include "local_fs.h"

#include <algorithm>
#include <cstdio>

#include <dirent.h>
#include <sys/stat.h>

namespace barch::localfs {
    bool is_dir(const std::string& path) {
        struct stat st{};
        return ::stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
    }

    bool is_reg(const std::string& path) {
        struct stat st{};
        return ::stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
    }

    bool read_file(const std::string& path, std::string& out) {
        FILE* f = ::fopen(path.c_str(), "rb");
        if (!f)
            return false;
        out.clear();
        char buf[65536];
        size_t n;
        while ((n = ::fread(buf, 1, sizeof buf, f)) > 0)
            out.append(buf, n);
        bool ok = ::ferror(f) == 0;
        ::fclose(f);
        return ok;
    }

    std::vector<std::string> list_dir(const std::string& path) {
        std::vector<std::string> names;
        DIR* d = ::opendir(path.c_str());
        if (!d)
            return names;
        while (auto* e = ::readdir(d)) {
            const char* n = e->d_name;
            if (n[0] == '.' && (n[1] == 0 || (n[1] == '.' && n[2] == 0)))
                continue;
            names.emplace_back(n);
        }
        ::closedir(d);
        std::sort(names.begin(), names.end());
        return names;
    }
}
