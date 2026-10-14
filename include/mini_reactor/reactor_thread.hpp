#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <unordered_map>

#include "fd_guard.hpp"
#include "mini_reactor/connection.hpp"
#include "mini_reactor/event_loop.hpp"
#include "mini_reactor/thread_pool.hpp"

class ReactorThread {
public:
  ReactorThread(uint16_t port, size_t reactor_idx,
                ThreadPool &global_thread_pool);
  ~ReactorThread();

  ReactorThread(const ReactorThread &) = delete;
  ReactorThread &operator=(const ReactorThread &) = delete;

  // 1. Lifecycle management
  void Start(); // Spawns the thread, binds socket, starts loop_.Run()
  void Stop();  // Calls loop_.Stop() + clean up

  // 2. Metrics & Accessors
  [[nodiscard]] size_t connection_count() const noexcept {
    return connection_count_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] size_t reactor_idx() const noexcept { return reactor_idx_; }
  [[nodiscard]] EventLoop &loop() noexcept { return loop_; }
  [[nodiscard]] ThreadPool &thread_pool() noexcept { return global_thread_pool_; }

private:
  void ThreadMain();
  void SetupClientListeningSocket();

  uint16_t port_;
  size_t reactor_idx_;
  ThreadPool &global_thread_pool_;
  UniqueFd listen_fd_; // listens on port_
  // OWNS THE OS THREAD + LOOP! Order matters!
  EventLoop loop_;
  std::jthread thread_;
  std::unordered_map<int, std::shared_ptr<Connection>> active_connections_;
  std::atomic<size_t> connection_count_{0};
};
