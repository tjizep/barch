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
    if (!port.empty() && barch::set_configuration_value("server_port", port) != 0) {
        std::cerr << argv[0] << ": bad port '" << port << "'\n";
        return 2;
    }
    if (!bind.empty() && barch::set_configuration_value("server_binding", bind) != 0) {
        std::cerr << argv[0] << ": bad bind address '" << bind << "'\n";
        return 2;
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

    auto listen_on = barch::get_server_binding();
    if (listen_on.empty())
        listen_on = "0.0.0.0";
    try {
        barch::server::start(listen_on, (uint_least16_t) barch::get_server_port(), false);
    } catch (const std::exception& e) {
        std::cerr << argv[0] << ": could not listen on " << listen_on << ":"
                  << barch::get_server_port() << ": " << e.what() << "\n";
        return 1;
    }
    barch::log({"barchd listening on", listen_on, barch::get_server_port()});

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
