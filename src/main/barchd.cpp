//
// barchd - barch as a program, with a RESP listener and no python in the process.
//
// The two other ways barch runs are a python extension the tests import and a valkey
// module. Both do the same start-up in their own idiom: ValkeyModule_OnLoad in
// src/barch.cpp, and the %init block in src/barch.i. This is that sequence written out
// once more, for a plain server: configuration from the environment and the command
// line, the function watcher if one is configured, the default key space, then the
// listener - and a signal handler that saves and stops rather than being killed
// mid-write. See TODO 237.
//
// Nothing here may call ValkeyModule_*. Those are function pointers a module server
// fills in, and outside one they are null - which is why register_valkey_configuration
// is in OnLoad and not in the shared start-up.
//
#include <atomic>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include <unistd.h>

#include <version.h>

#include "configuration.h"
#include "constants.h"
#include "fs_api.h"
#include "function_sync.h"
#include "logger.h"
#include "module.h"
#include "rpc/server.h"
#include "swig_api.h"

namespace {

std::mutex stop_mu;
std::condition_variable stop_cv;
std::atomic<int> stop_signal{0};

void on_signal(int sig) {
    // async-signal-safe: set a flag and wake the main thread, nothing else. Logging or
    // saving from inside a handler is how a shutdown turns into a deadlock
    stop_signal.store(sig, std::memory_order_release);
    stop_cv.notify_all();
}

void usage(const char* argv0) {
    std::cout <<
        "barch " BARCH_PROJECT_VERSION "\n"
        "\n"
        "usage: " << argv0 << " [options]\n"
        "\n"
        "  -p, --port N          listen on this port (default " << barch::get_server_port() << ")\n"
        "  -b, --bind ADDR       listen on this address (default "
                                 << (barch::get_server_binding().empty()
                                     ? std::string("0.0.0.0")
                                     : barch::get_server_binding()) << ")\n"
        "      --dir PATH        work here; barch writes its shards to the working\n"
        "                        directory, so this is where the data lives\n"
        "  -c, --config K=V      set a configuration value, repeatable. The same names\n"
        "                        CONFIG SET takes\n"
        "      --load-fs PATH[:ROOT][@SPACE] import a directory as the file store\n"
        "                        before listening, under ROOT (default /) and into\n"
        "                        SPACE (default the unnamed one). What LOADFS does\n"
        "      --load-keys PATH[:PREFIX][@SPACE] import a directory as keys and\n"
        "                        stored functions before listening. What LOADKEYS does\n"
        "      --no-save-on-exit exit without saving on SIGINT or SIGTERM\n"
        "  -v, --version         print the version and exit\n"
        "  -h, --help            this\n"
        "\n"
        "Every setting also reads BARCH_<NAME> from the environment - BARCH_MAX_MEMORY_BYTES\n"
        "for max_memory_bytes. The environment is applied first and --config overrides it.\n";
}

/** the value after an option, whether it was written --opt=value or --opt value */
bool take_value(int argc, char** argv, int& i, const char* eq, std::string& out) {
    if (eq) {
        out = eq + 1;
        return !out.empty();
    }
    if (i + 1 >= argc)
        return false;
    out = argv[++i];
    return true;
}

}

int main(int argc, char** argv) {
    std::string port;
    std::string bind;
    std::string dir;
    std::vector<std::string> settings;
    std::vector<std::string> load_fs;
    std::vector<std::string> load_keys;
    bool save_on_exit = true;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        const char* eq = std::strchr(argv[i], '=');
        std::string name = eq ? arg.substr(0, eq - argv[i]) : arg;
        std::string value;

        if (name == "-h" || name == "--help") {
            usage(argv[0]);
            return 0;
        }
        if (name == "-v" || name == "--version") {
            std::cout << BARCH_PROJECT_VERSION << "\n";
            return 0;
        }
        if (name == "--no-save-on-exit") {
            save_on_exit = false;
            continue;
        }
        if (name == "--load-fs" || name == "--load-keys") {
            std::string spec;
            if (!take_value(argc, argv, i, eq, spec)) {
                std::cerr << argv[0] << ": " << name << " wants a path\n";
                return 2;
            }
            (name == "--load-fs" ? load_fs : load_keys).push_back(spec);
            continue;
        }
        bool ok = true;
        if (name == "-p" || name == "--port")        ok = take_value(argc, argv, i, eq, port);
        else if (name == "-b" || name == "--bind")   ok = take_value(argc, argv, i, eq, bind);
        else if (name == "--dir")                    ok = take_value(argc, argv, i, eq, dir);
        else if (name == "-c" || name == "--config") {
            ok = take_value(argc, argv, i, eq, value);
            if (ok) settings.push_back(value);
        } else {
            std::cerr << argv[0] << ": unknown option '" << arg << "'\n";
            usage(argv[0]);
            return 2;
        }
        if (!ok) {
            std::cerr << argv[0] << ": " << name << " wants a value\n";
            return 2;
        }
    }

    // before anything reads a key space, since that is what fixes the data location
    if (!dir.empty() && ::chdir(dir.c_str()) != 0) {
        std::cerr << argv[0] << ": cannot work in '" << dir << "': "
                  << std::strerror(errno) << "\n";
        return 1;
    }

    // the environment first and the command line second, so an explicit --config wins
    // over an exported one - the same order the valkey module uses for its config file
    barch::apply_environment_configuration();
    for (const auto& kv : settings) {
        auto at = kv.find('=');
        if (at == std::string::npos) {
            std::cerr << argv[0] << ": --config wants name=value, got '" << kv << "'\n";
            return 2;
        }
        auto name = kv.substr(0, at);
        auto value = kv.substr(at + 1);
        if (barch::set_configuration_value(name, value) != 0) {
            std::cerr << argv[0] << ": cannot set " << name << " to '" << value << "'\n";
            return 2;
        }
    }
    /*
     * --port and --bind are kept here rather than written through
     * set_configuration_value, because setting `server_port` or `server_binding`
     * asynchronously restarts the server (configuration.cpp:422, through
     * restarter::asynch_restart). Doing that and then starting the listener below
     * built it up to three times on one boot, and on an early exit left a thread
     * starting a server while the process was tearing down - which is where the
     * `failed to start server std::bad_alloc` in a refused start-up came from.
     * See TODO 241.
     */
    uint_least16_t listen_port = (uint_least16_t) barch::get_server_port();
    if (!port.empty()) {
        auto n = strtoul(port.c_str(), nullptr, 10);
        if (n == 0 || n > 65535) {
            std::cerr << argv[0] << ": bad port '" << port << "'\n";
            return 2;
        }
        listen_port = (uint_least16_t) n;
    }

    // signals armed before the listener, so a SIGTERM arriving during start-up is not
    // the default disposition
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::signal(SIGPIPE, SIG_IGN);

    if (!barch::get_functions_dir().empty() && barch::get_functions_dir() != "off")
        barch::start_function_sync();

    // constructing the default key space is what loads the shards out of the working
    // directory and prints the banner
    if (get_default_ks() == nullptr) {
        std::cerr << argv[0] << ": no default key space\n";
        return 1;
    }

    /*
     * Imports before the listener opens: a directory that fails to load should stop
     * the server starting, not be discovered by the first request. Each is atomic in
     * itself - see TODO 238 - so a failure here leaves the space as it was.
     */
    /*
     * PATH[:SUFFIX][@SPACE], taken from the right: the space first, then the suffix,
     * so a path may contain either character and still parse. The suffix is a root
     * for --load-fs and a key prefix for --load-keys.
     */
    auto split_spec = [&](const std::string& spec, bool want_root,
                          std::string& path, std::string& suffix,
                          barch::key_space_ptr& space) -> std::string {
        path = spec;
        suffix.clear();
        auto at = path.rfind('@');
        std::string space_name;
        if (at != std::string::npos && at + 1 < path.size()) {
            space_name = path.substr(at + 1);
            path = path.substr(0, at);
        }
        auto colon = path.rfind(':');
        // for a root the colon has to be followed by a slash, so a path with a colon
        // in it is not mistaken for one. A prefix has no such shape to check
        if (colon != std::string::npos && colon + 1 < path.size() &&
            (!want_root || path[colon + 1] == '/')) {
            suffix = path.substr(colon + 1);
            path = path.substr(0, colon);
        }
        if (want_root && suffix.empty())
            suffix = "/";
        if (space_name.empty()) {
            space = get_default_ks();
            return {};
        }
        if (!barch::check_ks_name(space_name))
            return "'" + space_name + "' is not a key space name";
        space = barch::get_keyspace(space_name);
        if (!space)
            return "no key space '" + space_name + "'";
        return {};
    };

    for (const auto& spec : load_fs) {
        std::string path, root;
        barch::key_space_ptr space;
        auto err = split_spec(spec, true, path, root, space);
        std::vector<std::string> reply;
        if (err.empty())
            err = barch::load_fs_directory(path, root, 65536, space, reply);
        if (!err.empty()) {
            std::cerr << argv[0] << ": --load-fs " << spec << ": " << err << "\n";
            return 1;
        }
        std::string line;
        for (const auto& part : reply)
            line += (line.empty() ? "" : " ") + part;
        barch::log({"barchd imported", path, "as", root, "in", space->get_canonical_name(), line});
    }

    for (const auto& spec : load_keys) {
        std::string path, prefix;
        barch::key_space_ptr space;
        auto err = split_spec(spec, false, path, prefix, space);
        std::vector<std::string> reply;
        if (err.empty())
            err = barch::load_keys_directory(path, prefix, space, reply);
        if (!err.empty()) {
            std::cerr << argv[0] << ": --load-keys " << spec << ": " << err << "\n";
            return 1;
        }
        std::string line;
        for (const auto& part : reply)
            line += (line.empty() ? "" : " ") + part;
        barch::log({"barchd imported", path, "as keys in", space->get_canonical_name(), line});
    }

    auto listen_on = bind.empty() ? barch::get_server_binding() : bind;
    if (listen_on.empty())
        listen_on = "0.0.0.0";
    try {
        barch::server::start(listen_on, listen_port, false);
    } catch (const std::exception& e) {
        std::cerr << argv[0] << ": could not listen on " << listen_on << ":"
                  << listen_port << ": " << e.what() << "\n";
        return 1;
    }
    barch::log({"barchd listening on", listen_on, (uint64_t) listen_port});

    {
        std::unique_lock lock(stop_mu);
        stop_cv.wait(lock, [] { return stop_signal.load(std::memory_order_acquire) != 0; });
    }
    barch::log({"barchd stopping on signal", stop_signal.load()});

    barch::server::stop();
    if (save_on_exit) {
        // a database that loses the last minutes of writes because it was asked to stop
        // is not a good default. saveAll, not save: every space, not the default one
        barch::log({"barchd saving"});
        saveAll();
    }
    barch::log({"barchd stopped"});
    return 0;
}
