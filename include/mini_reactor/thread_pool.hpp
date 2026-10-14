#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <stop_token>
#include <thread>
#include <vector>

#include "fd_guard.hpp"

class EventLoop;

// Task classification mirroring libuv's uv__work_kind:
// - kCpu: CPU-heavy computation (e.g., hashing, encryption, compression)
// - kFastIo: Local filesystem operations (e.g., read, write, stat)
// - kSlowIo: Potentially unbounded blocking I/O (e.g., getaddrinfo / DNS
// lookups)
enum class WorkKind {
  kCpu,
  kFastIo,
  kSlowIo,
};

struct Job {
  std::function<void()> work; // empty `work` (nullptr) == sentinel "exit now"
  std::function<void()> on_done; // work to do on original event loop (clean up)
  EventLoop &eventloop;
  WorkKind kind = WorkKind::kCpu;
};

class ThreadPool {
  // All Event Loops share the same ThreadPool
public:
  ThreadPool(size_t n_threads);
  ~ThreadPool(); // pushes n_threads sentinel Jobs, notifies, joins

  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;

  // Runs `work` on a pool thread; `on_done` runs back on eventloop loop_'s
  // thread.
  void SubmitWork(EventLoop &loop, std::function<void()> work,
                  std::function<void()> on_done,
                  WorkKind kind = WorkKind::kCpu);

private:
  // Mirrors libuv's threadpool.c `worker()`: block on queue_cv_ until a Job
  // arrives, run it (or exit on a sentinel), never rely on stop_token alone
  // to unblock — a stop_token doesn't wake a plain condition_variable::wait,
  // so use condition_variable_any::wait(lock, stop_token, pred) or push an
  // explicit sentinel Job like libuv does. Either is fine; pick one.
  void WorkerLoop(std::stop_token stop_token);
  void OnWakeupFd(); // registered with loop_ via AddFd(event_fd_, ...)

  UniqueFd event_fd_;                 // eventfd(0, EFD_NONBLOCK)
  std::vector<std::jthread> workers_; // RAII join, same effect as libuv's
                                      // manual uv_thread_join() calls
  std::mutex queue_mu_;
  std::condition_variable_any queue_cv_; // _any: works with stop_token overload
  std::deque<Job> queue_;
  std::mutex results_mu_;
  std::deque<std::function<void()>> results_;
};
