//
// Created by linuxlite on 2/4/25.
//
#include <iostream>
#include <cstdlib>
#include <thread>
using namespace std;
static std::string valkey_cli = "_deps/valkey-src/src/valkey-cli";
static std::string ping_cmd = valkey_cli + " -e PING ";
static std::string failed = "failure: ";
static std::string binary_name = "_barch.so";
static unsigned max_iterations = 100;
int wait_to_stop(){
    if(0 != std::system("pkill -9 valkey-server")){
        std::cout << valkey_cli << " valkey-server was not started or an error occurred stopping it" << std::endl;
    }
    // wait for it to stop
    unsigned iters = max_iterations;
    std::cout << ping_cmd << std::endl;
    while (0 == std::system(ping_cmd.c_str())) {
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
    std::cout << ping_cmd << std::endl;
    while (0 != std::system(ping_cmd.c_str())) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (--iters == 0){
            std::cerr << failed << "server not started in time" << std::endl;
            return -1;
        }
    }
    return 0;
}
int main(int argc, char *argv[]) {
    if(argc != 3)
    {
        std::cerr << "Usage: " << argv[0] << " <directory containing "<< binary_name <<"> <lua test file>" << std::endl;
        return -1;
    }
    if ( 0 != wait_to_stop()) return -1;

    std::string bindir = argv[1];
    std::string luatest = argv[2];
    std::cout << "directory for : "<< binary_name << " " << bindir << std::endl;
    auto run_server = [&]()
    {
        cout << "server start" << endl;
        std::string server_cmd = "_deps/valkey-src/src/valkey-server --loadmodule ";
        server_cmd += bindir + "/";
        server_cmd += binary_name + " &";
        cout << server_cmd << endl;
        if (0 != std::system(server_cmd.c_str())) {
          std::cerr << failed << server_cmd << std::endl;
        }
    };
    std::thread t(run_server);
    std::string test_cmd = valkey_cli + " -e --eval ../" + luatest;
    std::cout << test_cmd << std::endl;

    cout << "waiting to start valkey...";
    if (wait_to_start() !=0) return -1;
    int r = 0;
    if(std::system(test_cmd.c_str()) != 0)
    {
        std::cerr << failed << test_cmd << std::endl;
        r = -1;
    }
    if (0!=wait_to_stop()){
        r = -1;
    }
    t.join();
    cout << "complete";
    return r;
}