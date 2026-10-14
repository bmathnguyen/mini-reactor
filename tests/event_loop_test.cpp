#include "mini_reactor/event_loop.hpp"
#include "mini_reactor/thread_pool.hpp"
#include "fd_guard.hpp"

#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;

// ----------------------------------------------------------------------------
// 1. UniqueFd RAII & Move Semantics
// ----------------------------------------------------------------------------
void TestUniqueFd() {
  std::printf("  [1] Running TestUniqueFd...\n");

  int pfd[2];
  assert(pipe(pfd) == 0);
  int read_raw = pfd[0];
  int write_raw = pfd[1];

  {
    UniqueFd u1(read_raw);
    assert(u1.valid());
    assert(u1.get() == read_raw);

    // Test Move Constructor: must steal fd and leave other invalid (-1)
    UniqueFd u2(std::move(u1));
    assert(u2.valid());
    assert(u2.get() == read_raw);
    assert(!u1.valid());
    assert(u1.get() == -1);

    // Test Move Assignment into default-constructed UniqueFd
    UniqueFd u3;
    u3 = std::move(u2);
    assert(u3.valid());
    assert(u3.get() == read_raw);
    assert(!u2.valid());
    assert(u2.get() == -1);

    // Test Move Assignment into already-valid UniqueFd (swap idiom transfers old fd to u3)
    UniqueFd u4(write_raw);
    u4 = std::move(u3);
    assert(u4.valid());
    assert(u4.get() == read_raw);
    // u3 now holds write_raw via swap; it will be closed when u3 destructs
    assert(u3.valid());
    assert(u3.get() == write_raw);
  }

  // Once scope exits, both read_raw and write_raw must be closed
  assert(fcntl(read_raw, F_GETFD) == -1 && errno == EBADF);
  assert(fcntl(write_raw, F_GETFD) == -1 && errno == EBADF);
  std::printf("      -> UniqueFd move semantics & auto-close verified.\n");
}

// ----------------------------------------------------------------------------
// 2. EventLoop Pipe I/O (Readable Notification)
// ----------------------------------------------------------------------------
void TestPipeIoReadable() {
  std::printf("  [2] Running TestPipeIoReadable...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  int pfd[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, pfd) == 0);
  UniqueFd rfd(pfd[0]);
  UniqueFd wfd(pfd[1]);

  // Set non-blocking on read end
  int flags = fcntl(rfd.get(), F_GETFL, 0);
  fcntl(rfd.get(), F_SETFL, flags | O_NONBLOCK);

  bool readable_fired = false;
  char read_buf[16] = {0};

  bool reg_ok = loop.RegisterFdEvent(
      rfd.get(), IoEvent::kReadable,
      [&](int fd, IoEvent mask) {
        assert((mask & IoEvent::kReadable) != IoEvent(0));
        ssize_t n = read(fd, read_buf, sizeof(read_buf) - 1);
        assert(n == 4);
        assert(std::strcmp(read_buf, "ping") == 0);
        readable_fired = true;

        // Clean up & stop loop
        loop.RemoveFdEvent(fd);
        loop.Stop();
      });
  assert(reg_ok);

  // Write payload to trigger kReadable
  assert(write(wfd.get(), "ping", 4) == 4);

  // Run event loop until Stop() is called inside the callback
  loop.Run();

  assert(readable_fired);
  std::printf("      -> Pipe I/O notification, payload read, and loop stop verified.\n");
}

// ----------------------------------------------------------------------------
// 3. Timer Priority Queue & Chronological Order
// ----------------------------------------------------------------------------
void TestTimerOrdering() {
  std::printf("  [3] Running TestTimerOrdering...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  std::vector<int> fire_order;

  // Register timers out of chronological order
  loop.RegisterTimerEvent(30ms, [&]() {
    fire_order.push_back(3);
    loop.Stop(); // Stop loop when slowest timer fires
  });

  loop.RegisterTimerEvent(10ms, [&]() {
    fire_order.push_back(1);
  });

  loop.RegisterTimerEvent(20ms, [&]() {
    fire_order.push_back(2);
  });

  loop.Run();

  assert(fire_order.size() == 3);
  assert(fire_order[0] == 1); // 10ms fired first
  assert(fire_order[1] == 2); // 20ms fired second
  assert(fire_order[2] == 3); // 30ms fired third
  std::printf("      -> Timers fired in strict chronological order [1 -> 2 -> 3].\n");
}

// ----------------------------------------------------------------------------
// 4. Timer Cancellation (Lazy Delete)
// ----------------------------------------------------------------------------
void TestTimerCancellation() {
  std::printf("  [4] Running TestTimerCancellation...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  bool timer_a_fired = false;
  bool timer_b_fired = false;
  bool timer_c_fired = false;

  loop.RegisterTimerEvent(10ms, [&]() {
    timer_a_fired = true;
  });

  TimerId id_b = loop.RegisterTimerEvent(20ms, [&]() {
    timer_b_fired = true; // MUST NOT FIRE
  });

  loop.RegisterTimerEvent(35ms, [&]() {
    timer_c_fired = true;
    loop.Stop();
  });

  // Cancel Timer B before it matures
  loop.CancelTimerEvent(id_b);

  loop.Run();

  assert(timer_a_fired == true);
  assert(timer_b_fired == false); // Verified cancelled timer was skipped
  assert(timer_c_fired == true);
  std::printf("      -> Cancelled timer skipped cleanly via lazy-delete.\n");
}

// ----------------------------------------------------------------------------
// 5. Dynamic Callback Array Growth
// ----------------------------------------------------------------------------
void TestDynamicFdGrowth() {
  std::printf("  [5] Running TestDynamicFdGrowth...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  int pfd[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, pfd) == 0);
  UniqueFd rfd(pfd[0]);
  UniqueFd wfd(pfd[1]);

  // Duplicate to a deliberately higher FD (e.g. 100) to force callbacks_.resize()
  int high_fd = dup2(rfd.get(), 100);
  assert(high_fd >= 100);
  UniqueFd high_u(high_fd);

  bool fired = false;
  bool ok = loop.RegisterFdEvent(high_fd, IoEvent::kReadable, [&](int, IoEvent) {
    fired = true;
    loop.Stop();
  });
  assert(ok);

  assert(write(wfd.get(), "x", 1) == 1);
  loop.Run();

  assert(fired);
  loop.RemoveFdEvent(high_fd);
  std::printf("      -> Dynamic callbacks array resized for high fd=%d.\n", high_fd);
}

// ----------------------------------------------------------------------------
// 6. Writable Event Notification & Flow
// ----------------------------------------------------------------------------
void TestPipeIoWritable() {
  std::printf("  [6] Running TestPipeIoWritable...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  int pfd[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, pfd) == 0);
  UniqueFd rfd(pfd[0]);
  UniqueFd wfd(pfd[1]);

  bool writable_fired = false;

  bool ok = loop.RegisterFdEvent(
      wfd.get(), IoEvent::kWritable,
      [&](int fd, IoEvent mask) {
        assert((mask & IoEvent::kWritable) != IoEvent(0));
        assert(fd == wfd.get());
        writable_fired = true;
        loop.RemoveFdEvent(fd);
        loop.Stop();
      });
  assert(ok);

  loop.Run();
  assert(writable_fired);
  std::printf("      -> Writable event fired as expected on non-full socket buffer.\n");
}

// ----------------------------------------------------------------------------
// 7. Dynamic Mask Modification (ModifyFdEvent)
// ----------------------------------------------------------------------------
void TestModifyFdEvent() {
  std::printf("  [7] Running TestModifyFdEvent...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  int pfd[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, pfd) == 0);
  UniqueFd rfd(pfd[0]);
  UniqueFd wfd(pfd[1]);

  int state_step = 0;

  bool ok = loop.RegisterFdEvent(
      rfd.get(), IoEvent::kReadable,
      [&](int fd, IoEvent mask) {
        if (state_step == 0) {
          // 1st fire: Readable triggered
          assert((mask & IoEvent::kReadable) != IoEvent(0));
          char buf[8];
          assert(read(fd, buf, 1) == 1);
          state_step = 1;

          // Now modify mask to also track kWritable on the same fd
          bool mod_ok = loop.ModifyFdEvent(fd, IoEvent::kReadable | IoEvent::kWritable);
          assert(mod_ok);
        } else if (state_step == 1) {
          // 2nd fire: kWritable triggered after modification
          assert((mask & IoEvent::kWritable) != IoEvent(0));
          state_step = 2;
          loop.RemoveFdEvent(fd);
          loop.Stop();
        }
      });
  assert(ok);

  // Trigger readable event first
  assert(write(wfd.get(), "a", 1) == 1);
  loop.Run();

  assert(state_step == 2);
  std::printf("      -> ModifyFdEvent successfully changed poller mask dynamically.\n");
}

// ----------------------------------------------------------------------------
// 8. Immediate Zero-Timeout Timer
// ----------------------------------------------------------------------------
void TestZeroTimeoutImmediateTimer() {
  std::printf("  [8] Running TestZeroTimeoutImmediateTimer...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  bool zero_timer_fired = false;

  loop.RegisterTimerEvent(0ms, [&]() {
    zero_timer_fired = true;
    loop.Stop();
  });

  loop.Run();
  assert(zero_timer_fired);
  std::printf("      -> 0ms delay timer executed immediately on first loop pass.\n");
}

// ----------------------------------------------------------------------------
// 9. Multiple Concurrent FDs on Single Loop
// ----------------------------------------------------------------------------
void TestMultipleFdsConcurrent() {
  std::printf("  [9] Running TestMultipleFdsConcurrent...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  constexpr int kNumPairs = 5;
  UniqueFd rfds[kNumPairs];
  UniqueFd wfds[kNumPairs];
  int fired_mask = 0;

  for (int i = 0; i < kNumPairs; ++i) {
    int pfd[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, pfd) == 0);
    rfds[i] = UniqueFd(pfd[0]);
    wfds[i] = UniqueFd(pfd[1]);

    int id = i;
    bool ok = loop.RegisterFdEvent(
        rfds[i].get(), IoEvent::kReadable,
        [&, id](int fd, IoEvent) {
          char buf[8];
          ssize_t n = read(fd, buf, sizeof(buf));
          assert(n > 0);
          fired_mask |= (1 << id);
          loop.RemoveFdEvent(fd);
          if (fired_mask == ((1 << kNumPairs) - 1)) {
            loop.Stop();
          }
        });
    assert(ok);
  }

  // Write payload to all pairs
  for (int i = 0; i < kNumPairs; ++i) {
    assert(write(wfds[i].get(), "ping", 4) == 4);
  }

  loop.Run();
  assert(fired_mask == ((1 << kNumPairs) - 1));
  std::printf("      -> All %d concurrent sockets fired and processed without cross-talk.\n", kNumPairs);
}

int main() {
  std::puts("=== mini_reactor EventLoop & Timer Comprehensive Test Suite ===");
  TestUniqueFd();
  TestPipeIoReadable();
  TestTimerOrdering();
  TestTimerCancellation();
  TestDynamicFdGrowth();
  TestPipeIoWritable();
  TestModifyFdEvent();
  TestZeroTimeoutImmediateTimer();
  TestMultipleFdsConcurrent();
  std::puts("=== All 9 EventLoop unit tests passed successfully! ===\n");
  return 0;
}
