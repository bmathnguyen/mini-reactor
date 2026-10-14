#pragma once
#include <unistd.h>
#include <utility>

// Internal RAII wrapper around a raw file descriptor. Lives under src/, not
// include/mini_reactor/ — a user linking mini_reactor should never need to
// see UniqueFd directly, only the public classes that hold one privately
// (EventLoop, Connection, ThreadPool, Server). Small and self-contained
// enough to stay header-only; there's no matching fd_guard.cpp.
class UniqueFd {
private:
  int fd_ = -1;

public:
  UniqueFd() noexcept = default;
  explicit UniqueFd(int fd) noexcept : fd_(fd) {}

  ~UniqueFd() {
    if (fd_ == -1) {
      return;
    }
    close(fd_);
  }

  UniqueFd(const UniqueFd &) = delete;
  UniqueFd &operator=(const UniqueFd &) = delete;

  UniqueFd(UniqueFd &&other) noexcept : fd_{other.fd_} {
    // move constructor
    other.fd_ = -1;
  }

  UniqueFd &operator=(UniqueFd &&other) noexcept {
    // move assignment
    if (this == &other) {
      return *this;
    }
    std::swap(fd_, other.fd_); // other will close the current fd_ correctly!
    return *this;
  }

  [[nodiscard]] int get() const noexcept { return fd_; }
  [[nodiscard]] bool valid() const noexcept { return fd_ >= 0; }

  int release() noexcept {
    int old = fd_;
    fd_ = -1;
    return old;
  }

  void reset(int new_fd = -1) noexcept {
    if (fd_ >= 0) {
      close(fd_);
    }
    fd_ = new_fd;
  }
};
