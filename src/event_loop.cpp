#include "mini_reactor/event_loop.hpp"
#include "poller.hpp"
#include <atomic>
#include <chrono>

constexpr int CALLBACK_INIT_SIZE = 64;
constexpr int READY_INIT_SIZE = 128;

EventLoop::EventLoop(ThreadPool &global_thread_pool)
    : global_thread_pool_(global_thread_pool),
      poller_(std::make_unique<Poller>()), callbacks_(CALLBACK_INIT_SIZE),
      ready_fds_(READY_INIT_SIZE), ready_masks_(READY_INIT_SIZE) {}

EventLoop::~EventLoop() { Stop(); }

bool EventLoop::RegisterFdEvent(int fd, IoEvent mask, IoCallback callback) {
  EventLoop::GrowCallbacksIfNeeded(fd);
  if (!poller_->Register(fd, mask)) {
    return false; // failed to register to OS polling
  }
  callbacks_[fd] = FdEvent{mask, std::move(callback)};
  return true;
}

bool EventLoop::ModifyFdEvent(int fd, IoEvent mask) {
  return poller_->Modify(fd, mask);
}

bool EventLoop::RemoveFdEvent(int fd) {
  if (!poller_->Remove(fd)) {
    return false;
  }
  // clean up callbacks_
  callbacks_[fd] = {};
  return true;
}

TimerId generateId() {
  static std::atomic<int> id = 0;
  return id.fetch_add(1, std::memory_order_relaxed);
}

TimerId EventLoop::RegisterTimerEvent(std::chrono::milliseconds delay,
                                      TimerCallback cb) {
  TimerId id = generateId();
  timers_.push(TimerEvent{id, std::chrono::steady_clock::now() + delay, cb});
  return id;
}

void EventLoop::CancelTimerEvent(TimerId id) {
  // lazy delete, set is_cancelled = true, i.e. add to cancelled set
  cancelled.insert(id);
}

void EventLoop::Run() {
  is_running_.store(true, std::memory_order_release);
  while (is_running_.load(std::memory_order_acquire)) {
    // do work
    auto max_time_out = NextTimeout();
    auto n = poller_->Poll(max_time_out, ready_fds_, ready_masks_);
    if (n < 0) {
      if (errno == EINTR) {
        continue; // interrupted by signal, try again
      } else {
        break; // real error, exit loop
      }
    }
    if (n > 0) {
      ProcessReadyFdEvents(n);
    }
    // handle timers when there are no ready fds or when the polling timed
    // out
    ProcessTimers();
  }
}

void EventLoop::Stop() noexcept {
  is_running_.store(false, std::memory_order_release);
}

// returns the next TimeOut for all the timers event
std::chrono::milliseconds EventLoop::NextTimeout() const {
  // NOTE: Cap max timeout to 50ms so that cross-thread Stop() unblocks the poller cleanly
  constexpr auto kMaxIdlePollTimeout = std::chrono::milliseconds{50};
  if (timers_.empty()) {
    return kMaxIdlePollTimeout;
  } else {
    const auto now = std::chrono::steady_clock::now();
    const auto deadline = timers_.top().deadline;
    auto remaining = deadline > now
                         ? std::chrono::duration_cast<std::chrono::milliseconds>(
                               deadline - now)
                         : std::chrono::milliseconds{0};
    return std::min(remaining, kMaxIdlePollTimeout);
  }
}

void EventLoop::ProcessTimers() {
  while (!timers_.empty() &&
         timers_.top().deadline <= std::chrono::steady_clock::now()) {
    TimerEvent t = timers_.top();
    timers_.pop();
    if (cancelled.contains(t.id)) {
      // delete from cancelled and skip
      cancelled.erase(t.id);
      continue; // skip cancelled tasks
    }
    // do this synchronously on main event loop thread
    if (t.callback) {
      t.callback();
    }
  }
}

void EventLoop::ProcessReadyFdEvents(int n) {
  for (int i = 0; i < n; ++i) {
    int fd = ready_fds_[i];
    IoEvent mask = ready_masks_[i]; // Indexed by i, matching ready_fds_[i]
    if (static_cast<size_t>(fd) < callbacks_.size()) {
      auto cb = callbacks_[fd].callback;
      if (cb) {
        cb(fd, mask);
      }
    }
  }
}

void EventLoop::GrowCallbacksIfNeeded(size_t fd) {
  if (fd >= callbacks_.size()) {
    callbacks_.resize(
        std::max(callbacks_.size() * 2, static_cast<size_t>(fd + 1)));
  }
}
