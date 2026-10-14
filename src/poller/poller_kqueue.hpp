#pragma once

#include <chrono>
#include <sys/event.h>
#include <sys/time.h>
#include <unordered_map>
#include <vector>

#include "fd_guard.hpp"
#include "mini_reactor/event_loop.hpp"

// Wrapper API around OS polling mechanisms (epoll/kqueue)
class Poller {
private:
  UniqueFd kqfd_;
  std::vector<struct kevent> scratch_;       // reused across Wait() calls
  std::unordered_map<int, IoEvent> pending_; // reused scratch for coalescing

  [[nodiscard]] bool ApplyMask(int fd, IoEvent mask) noexcept;

public:
  Poller();
  ~Poller() = default; // calls the destructor for all the class members in
                       // reverse order

  Poller(const Poller &) = delete;
  Poller &operator=(const Poller &) = delete;

  [[nodiscard]] bool Register(int fd, IoEvent mask) noexcept;
  [[nodiscard]] bool Modify(int fd, IoEvent mask) noexcept;
  bool Remove(int fd) noexcept;
  [[nodiscard]] int Poll(std::chrono::milliseconds timeout,
                         std::vector<int> &ready_fds,
                         std::vector<IoEvent> &ready_masks);
};