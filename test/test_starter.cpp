//
// Created by linuxlite on 2/4/25.
//
#include <iostream>
#include <cstdlib>
#include <thread>
using namespace std;

int main(int argc, char *argv[]) {
    if(argc != 2)
    {
        std::cerr << "Usage: " << argv[0] << " <directory containing libcdict.so>" << std::endl;
        return -1;
    }
    std::string bindir = argv[1];
    std::cout << "directory for : libcdict.so " << bindir << std::endl;
    auto run_server = [&]()
    {
        cout << "server start" << endl;
        std::string server_cmd = "_deps/valkey-src/src/valkey-server --loadmodule ";
        server_cmd += bindir + "/";
        server_cmd += "libcdict.so";
        cout << server_cmd << endl;
        std::system(server_cmd.c_str());
    };
    std::thread t(run_server);

    cout << "waiting to start valkey...";
    std::string test_cmd = "_deps/valkey-src/src/valkey-cli --eval ../test.lua";
    std::cout << test_cmd << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    if(std::system(test_cmd.c_str()) != 0)
    {
        std::cerr << test_cmd << std::endl;
        return -1;
    }
    cout << "complete";
    t.join();
    return 0;
}