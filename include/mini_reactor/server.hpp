#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stop_token>
#include <thread>
#include <vector>

#include "fd_guard.hpp"
#include "reactor_thread.hpp"
#include "thread_pool.hpp"

struct ServerConfig {
  uint16_t port;
  size_t n_reactors = std::thread::hardware_concurrency();
  size_t n_thread_pool_workers = 4; // global thread pool
  std::chrono::seconds idle_timeout{60};
  size_t max_read_buffer_bytes = 1 << 20;
};

class Server {
public:
  explicit Server(ServerConfig config);
  void Run();  // spawns n_reactors jthreads, each: bind+listen+EventLoop::Run()
  void Stop(); // requests stop on every reactor's stop_token

private:
  ServerConfig config_;
  ThreadPool global_thread_pool_;
  std::vector<std::unique_ptr<ReactorThread>> reactors_;
};
