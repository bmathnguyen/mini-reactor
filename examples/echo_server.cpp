// Example program using mini_reactor's public API to demonstrate connecting clients and dispatching work.
#include "mini_reactor/event_loop.hpp"
#include "mini_reactor/thread_pool.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <thread>

// Helper to set socket to non-blocking
void SetNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int main() {
    ThreadPool pool(4); // 4 background workers
    EventLoop loop(pool);
    
    // 1. Create a listening socket
    UniqueFd listen_fd(socket(AF_INET, SOCK_STREAM, 0));
    int opt = 1;
    setsockopt(listen_fd.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    SetNonBlocking(listen_fd.get());
    
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);
    
    if (bind(listen_fd.get(), (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Bind failed\n";
        return 1;
    }
    listen(listen_fd.get(), 128);
    std::cout << "Listening on port 8080...\n";
    std::cout << "Run: echo 'Hello Server' | nc localhost 8080\n";
    
    // 2. Register the listening socket with the EventLoop
    (void)loop.RegisterFdEvent(listen_fd.get(), IoEvent::kReadable, [&](int fd, IoEvent /*mask*/) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_sock = accept(fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_sock < 0) return;
        
        SetNonBlocking(client_sock);
        std::cout << "Client connected: fd " << client_sock << "\n";
        
        // 3. Register the new client socket for reading
        (void)loop.RegisterFdEvent(client_sock, IoEvent::kReadable, [&pool, &loop](int c_fd, IoEvent /*mask*/) {
            char buffer[1024];
            ssize_t bytes = read(c_fd, buffer, sizeof(buffer) - 1);
            if (bytes <= 0) {
                std::cout << "Client disconnected: fd " << c_fd << "\n";
                loop.RemoveFdEvent(c_fd);
                close(c_fd);
                return;
            }
            buffer[bytes] = '\0';
            std::string request_data(buffer);
            std::cout << "Read from client: " << request_data;
            
            // 4. DISPATCH WORK TO THE THREAD POOL!
            pool.SubmitWork(
                loop,
                /*work=*/[request_data]() {
                    std::cout << "[Worker Thread] Processing heavy data for 500ms...\n";
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                },
                /*on_done=*/[c_fd, request_data]() {
                    std::cout << "[Main Thread] Finished processing, sending response.\n";
                    std::string response = "Echo processed: " + request_data;
                    write(c_fd, response.c_str(), response.length());
                },
                WorkKind::kCpu
            );
        });
    });
    
    // 5. Start the Reactor!
    loop.Run();
    return 0;
}
