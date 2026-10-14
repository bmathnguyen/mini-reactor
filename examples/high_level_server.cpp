#include "mini_reactor/server.hpp"
#include "mini_reactor/connection.hpp"

#include <iostream>
#include <csignal>

// Global pointer for graceful shutdown via SIGINT (Ctrl+C)
Server* g_server = nullptr;

void HandleSignal(int) {
    if (g_server) {
        std::cout << "\nStopping server...\n";
        g_server->Stop();
    }
}

int main() {
    // 1. Configure the high-level server
    ServerConfig config;
    config.port = 8080;
    
    // Phase 6: Multi-reactor - Run an EventLoop on every CPU core!
    config.n_reactors = std::thread::hardware_concurrency(); 
    
    // Phase 5: Thread Pool - 4 background threads per reactor for slow work
    config.n_thread_pool_workers = 4; 

    // 2. Initialize the Server
    Server server(config);
    g_server = &server;
    std::signal(SIGINT, HandleSignal);

    // =========================================================================
    // TODO FOR YOU (Framework Design):
    // Right now, `Server` and `Connection` don't have a way to accept custom 
    // application logic from the user. 
    // 
    // As you build out the framework, you should add an interface to `Server`
    // (e.g., `server.SetConnectionHandler(...)`) so the user can inject their 
    // business logic. It would look something like this in the future:
    //
    // server.SetConnectionHandler([](Connection& conn, std::span<const std::byte> data) {
    //     std::cout << "Received data from fd " << conn.fd() << "\n";
    //     
    //     // The Connection object hides all the messy read/write OS buffers.
    //     // You simply call Write() with your response!
    //     conn.Write(data); 
    // });
    // =========================================================================
    
    std::cout << "Starting high-level Server on port " << config.port << " with " 
              << config.n_reactors << " reactors...\n";

    // 3. Run blocks the main thread. It spawns the reactor threads, 
    // each binding to the port with SO_REUSEPORT and running its own EventLoop.
    server.Run();

    std::cout << "Server stopped gracefully.\n";
    return 0;
}
