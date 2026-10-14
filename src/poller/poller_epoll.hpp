#pragma once
#if defined(__linux__)

#include <chrono>
#include <cstdint>
#include <vector>
#include <sys/epoll.h>

#include "fd_guard.hpp"
#include "event_loop.hpp"  // for IoEvent

class Poller {
 public:
  Poller();
  ~Poller() = default;

  Poller(const Poller&) = delete;
  Poller& operator=(const Poller&) = delete;

  [[nodiscard]] bool Add(int fd, IoEvent mask) noexcept;
  [[nodiscard]] bool Mod(int fd, IoEvent mask) noexcept;
  bool Remove(int fd) noexcept;

  // Blocks up to `timeout` (negative = block forever). Returns the number
  // of ready fds written into out_fds/out_masks (parallel arrays), or -1
  // on a real error (check errno).
  [[nodiscard]] int Wait(std::chrono::milliseconds timeout,
                          std::vector<int>& out_fds,
                          std::vector<IoEvent>& out_masks);

 private:
  static uint32_t ToEpollMask(IoEvent mask) noexcept;
  static IoEvent FromEpollMask(uint32_t epoll_mask) noexcept;

  UniqueFd epfd_;
  std::vector<epoll_event> scratch_;  // reused across Wait() calls
};

#endif