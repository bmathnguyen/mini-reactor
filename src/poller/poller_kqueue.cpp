#include "poller_kqueue.hpp"

#include <cerrno>
#include <stdexcept>

namespace {
constexpr size_t kInitialScratchSize = 64;
}

Poller::Poller() : kqfd_(::kqueue()) {
  if (!kqfd_.valid()) {
    throw std::runtime_error("kqueue() failed");
  }
  scratch_.resize(kInitialScratchSize);
}

bool Poller::ApplyMask(int fd, IoEvent mask) noexcept {
  struct kevent changes[2];
  int n = 0;

  if ((mask & IoEvent::kReadable) != IoEvent{}) {
    EV_SET(&changes[n++], fd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_RECEIPT, 0, 0, nullptr);
  } else {
    EV_SET(&changes[n++], fd, EVFILT_READ, EV_DELETE | EV_RECEIPT, 0, 0, nullptr);
  }

  if ((mask & IoEvent::kWritable) != IoEvent{}) {
    EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE | EV_RECEIPT, 0, 0, nullptr);
  } else {
    EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_DELETE | EV_RECEIPT, 0, 0, nullptr);
  }

  struct kevent events[2];
  struct timespec zero_ts{0, 0};
  int result = ::kevent(kqfd_.get(), changes, n, events, n, &zero_ts);
  if (result < 0) {
    return false;
  }
  for (int i = 0; i < result; ++i) {
    if ((events[i].flags & EV_ERROR) && events[i].data != 0 && events[i].data != ENOENT) {
      return false;
    }
  }
  return true;
}

bool Poller::Register(int fd, IoEvent mask) noexcept {
  return ApplyMask(fd, mask);
}
bool Poller::Modify(int fd, IoEvent mask) noexcept {
  return ApplyMask(fd, mask);
}

bool Poller::Remove(int fd) noexcept { return ApplyMask(fd, IoEvent{}); }

int Poller::Poll(std::chrono::milliseconds timeout, std::vector<int> &out_fds,
                 std::vector<IoEvent> &out_masks) {
  struct timespec ts {};
  struct timespec *ts_ptr = nullptr;
  if (timeout.count() >= 0) {
    ts.tv_sec = timeout.count() / 1000;
    ts.tv_nsec = (timeout.count() % 1000) * 1'000'000;
    ts_ptr = &ts;
  }

  int n;
  do {
    n = ::kevent(kqfd_.get(), nullptr, 0, scratch_.data(),
                 static_cast<int>(scratch_.size()), ts_ptr);
  } while (n < 0 && errno == EINTR);

  if (n < 0) {
    return -1;
  }

  pending_.clear();
  for (int i = 0; i < n; ++i) {
    const auto &ev = scratch_[i];
    const int fd = static_cast<int>(ev.ident);
    IoEvent bits{};
    if (ev.filter == EVFILT_READ) {
      bits = bits | IoEvent::kReadable;
    } else if (ev.filter == EVFILT_WRITE) {
      bits = bits | IoEvent::kWritable;
    }
    if (ev.flags & EV_EOF) {
      bits = bits | IoEvent::kReadable | IoEvent::kWritable;
    }
    pending_[fd] = pending_[fd] | bits;
  }

  out_fds.clear();
  out_masks.clear();
  for (const auto &[fd, mask] : pending_) {
    out_fds.push_back(fd);
    out_masks.push_back(mask);
  }

  if (n == static_cast<int>(scratch_.size())) {
    scratch_.resize(scratch_.size() * 2);
  }

  return static_cast<int>(out_fds.size());
}