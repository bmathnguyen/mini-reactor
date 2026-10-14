#include "mini_reactor/server.hpp"
#include "fd_guard.hpp"
#include "mini_reactor/connection.hpp"
#include "mini_reactor/event_loop.hpp"
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

Server::Server(ServerConfig config)
    : config_(std::move(config)),
      global_thread_pool_(ThreadPool(config.n_thread_pool_workers)) {}

void Server::Run() {
  // Spawn N reactor threads to the background.
  // Each reactor is bound to a unique listening socket on port.
  // Requests are distributed among threads using OS's kernels distribution.
  for (size_t i = 0; i < config_.n_reactors; ++i) {
    reactors_.emplace_back(
        std::make_unique<ReactorThread>(config_.port, i, global_thread_pool_));
    reactors_.back()->Start();
  }
}

void Server::Stop() {
  for (auto &reactor_ptr : reactors_) {
    reactor_ptr->Stop();
  }
}
