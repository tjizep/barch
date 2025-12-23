//
// Created by linuxlite on 2/4/25.
//
#include <iostream>
#include <cstdlib>
#include <thread>
#include <filesystem>

using namespace std;
static std::string test_build_dir = "";
static std::string valkey_path = "/_deps/valkey-src/src/";
static std::string valkey_cli = valkey_path + "valkey-cli -p 7777";
static std::string ping_cmd = valkey_cli + " -e PING ";
static std::string shutdown_cmd = valkey_cli + " -e SHUTDOWN ";
static std::string failed = "failure: ";
static std::string binary_name = "_barch.so";
static unsigned max_iterations = 20;
static std::string get_ping_cmd() {
    std::string full_ping_cmd = test_build_dir + ping_cmd;
    return full_ping_cmd;
}
static std::string get_shutdown_cmd() {
    std::string full_ping_cmd = test_build_dir + shutdown_cmd;
    return full_ping_cmd;
}
int wait_to_stop(){

    if(0 != std::system(get_shutdown_cmd().c_str())){
        std::cout << valkey_cli << " valkey-server was not started or an error occurred stopping it" << std::endl;
    }
    // wait for it to stop
    unsigned iters = max_iterations;

    std::cout << get_ping_cmd() << std::endl;

    while (0 == std::system(get_ping_cmd().c_str())) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (--iters == 0){
            std::cerr << failed << "server not stopped in time" << std::endl;
            return -1;
        }
    }
    return 0;
}
int wait_to_start() {
    // wait for the valkey-server to start
    unsigned iters = max_iterations;

    std::cout << "ping command: " << get_ping_cmd() << std::endl;
    while (0 != std::system(get_ping_cmd().c_str())) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (--iters == 0){
            std::cerr << failed << "server not started in time" << std::endl;
            return -1;
        }
    }
    return 0;
}
int main(int argc, char *argv[]) {
    if(argc != 4)
    {
        std::cerr << "Usage: " << argv[0] << " <directory containing "<< binary_name <<"> <lua test file>" << std::endl;
        return -1;
    }
    if ( 0 != wait_to_stop()) return -1;

    std::string bindir = argv[1];
    std::string luatest = argv[2];
    test_build_dir = argv[3];

    std::cout << "directory for : "<< binary_name << " " << bindir << std::endl;
    std::cout << "directory for test build : " << test_build_dir << std::endl;
    std::filesystem::current_path(bindir);
    std::cout << "working directory : " << std::filesystem::current_path() << std::endl;

    auto run_server = [&]()
    {

        cout << "server start in " << std::filesystem::current_path() << endl;
        std::string server_cmd = test_build_dir + valkey_path + "valkey-server --port 7777 --loadmodule ";
        server_cmd += bindir + "/";
        server_cmd += binary_name + " &";
        std::cout << "Starting server: " << server_cmd << endl;
        if (0 != std::system(server_cmd.c_str())) {
          std::cerr << failed << server_cmd << std::endl;
        }
    };
    std::thread t(run_server);
    std::string test_cmd = test_build_dir +valkey_cli + " -e --eval " + luatest;
    std::cout << "Command to run test " << test_cmd << std::endl;

    cout << "waiting to start valkey...";
    if (wait_to_start() !=0) return -1;
    int r = 0;
    std::cout << "---------- Command Output ----------------" << std::endl;
    if(std::system(test_cmd.c_str()) != 0)
    {
        std::cerr << failed << test_cmd << std::endl;
        r = -1;
    }
    std::cout << "------------------------------------------" << std::endl;
    if (0!=wait_to_stop()){
        r = -1;
    }
    t.join();
    cout << "complete";
    return r;
}