#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <queue>
#include <unordered_set>
#include <vector>

#include "fd_guard.hpp"
#include "thread_pool.hpp"

// NOTE: bitmask enum to specify if the fd is readable and/or writable
// also used for epoll_wait() to know what's actually ready
enum class IoEvent : uint32_t { kReadable = 1u << 0, kWritable = 1u << 1 };
// a bit boilerplate here
constexpr IoEvent operator|(IoEvent a, IoEvent b) noexcept {
  return static_cast<IoEvent>(static_cast<uint32_t>(a) |
                              static_cast<uint32_t>(b));
}
constexpr IoEvent operator&(IoEvent a, IoEvent b) noexcept {
  return static_cast<IoEvent>(static_cast<uint32_t>(a) &
                              static_cast<uint32_t>(b));
}
constexpr bool operator!=(IoEvent a, IoEvent b) noexcept {
  return static_cast<uint32_t>(a) != static_cast<uint32_t>(b);
}
// struct TimerId { uint64_t value; };   // strong type, not a bare uint64_t
// NOTE: can just use TimerId = uint64_t
using TimerId = uint64_t;

// Callback = Work to do! Will be dispatched to Threads in the Thread pool
// (either Local or Global Thread pool)
using IoCallback = std::function<void(int fd, IoEvent mask)>;
using TimerCallback = std::function<void()>;

struct FdEvent {
  IoEvent mask{};
  IoCallback callback;
};

struct TimerEvent {
  TimerId id;
  std::chrono::steady_clock::time_point deadline;
  TimerCallback callback;
  // bool is_cancelled = false; // track separately
};

struct TimerEventComp {
  // returns true iff a is std::less (less important) than b, similar to
  // std::less a less important than b iff a deadline is further than b
  bool operator()(const TimerEvent &a, const TimerEvent &b) const {
    return a.deadline > b.deadline;
  }
};

using TimerQueue =
    std::priority_queue<TimerEvent, std::vector<TimerEvent>, TimerEventComp>;

// this is forward declaration!
class Poller; // NOT extern, extern is for linkage specifier: variables and
              // functions

// NOTE: each Server will launch n Reactor threads (i.e. EventLoop threads) to
// handle all the connections reason is: to Increase the Throughput by
// maximizing all the Cores in 1 machine! (true Parallelism) Redis: only 1
// Reactor thread, since GET/SET is not CPU-intensive Cost when using multiple
// Reactor threads: locking data structures nginx/ Envoy: CPU gets easily
// saturated, needs multiple Reactor threads!
class EventLoop {
public:
  EventLoop(ThreadPool &global_thread_pool); // Init data structures, inject
                                             // thread pool + epoll OS
                                             // resources (via pimple idiom with
                                             // Poller)
  ~EventLoop();

  EventLoop(const EventLoop &) = delete;
  EventLoop &operator=(const EventLoop &) = delete;

  // 1. Register IoCallback work
  // Register/modify/remove interest in an fd.
  [[nodiscard]] bool RegisterFdEvent(
      int fd, IoEvent mask,
      IoCallback cb); // register the fd with its callback work: FdEvent
  [[nodiscard]] bool ModifyFdEvent(
      int fd,
      IoEvent mask); // overwriting the mask, used for always writable fd
  bool RemoveFdEvent(int fd); // remove

  // 2. Register TimerCallback work
  // Schedule `cb` to run after `delay`. Returns an id usable with CancelTimer.
  TimerId
  RegisterTimerEvent(std::chrono::milliseconds delay,
                     TimerCallback cb); // will be associated with TimerId
  void CancelTimerEvent(TimerId id);

  // Blocks, dispatching fd and timer callbacks until Stop() is called.
  void Run();
  void Stop() noexcept;
  ThreadPool &GetGlobalThreadPool() { return global_thread_pool_; }

private:
  [[nodiscard]] std::chrono::milliseconds
  NextTimeout() const; // like usUntilEarliestTimer in ae.c
  void ProcessTimers();
  void ProcessReadyFdEvents(
      int n); // FiredEvents in ae.c, n = return value of epoll_wait
  void GrowCallbacksIfNeeded(
      size_t fd); // doubling growth, mirrors aeCreateFileEvent

  ThreadPool &global_thread_pool_;
  std::unique_ptr<Poller> poller_; // only for FdEvent
  // NOTE: use a vector<> here for storing [fd_number], similar to ae.c's
  // events[fd] Resizes like aeCreateFileEvent's `newnevents = newnevents * 2`.
  std::vector<FdEvent> callbacks_;
  TimerQueue timers_; // NOTE: DESIGN DECISION: use pq instead of vector (Redis
                      // uses vector due to small #timer events)
  std::unordered_set<TimerId> cancelled; // NOTE: DESIGN DECISION: keep track of
                                         // is_cancelled in a separate hashset
  std::atomic<bool> is_running_{false};  // for stopping the event loop
  std::vector<int> ready_fds_;           // from OS epoll Poll!
  std::vector<IoEvent>
      ready_masks_; // from Poll as well! storing same order as ready_fds_
};
