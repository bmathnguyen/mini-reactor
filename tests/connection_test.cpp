#include "mini_reactor/connection.hpp"
#include "mini_reactor/event_loop.hpp"
#include "mini_reactor/thread_pool.hpp"
#include "fd_guard.hpp"

#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;

static void SetNonBlocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  assert(flags >= 0);
  assert(fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0);
}

// ----------------------------------------------------------------------------
// 1. Connection Creation, Initial State, and Accessors
// ----------------------------------------------------------------------------
void TestConnectionInitialState() {
  std::printf("  [1] Running TestConnectionInitialState...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  int sv[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
  SetNonBlocking(sv[0]);
  SetNonBlocking(sv[1]);

  UniqueFd peer_fd(sv[1]);
  int conn_raw_fd = sv[0];

  auto conn = std::make_shared<Connection>(UniqueFd(sv[0]), loop);

  assert(conn->fd() == conn_raw_fd);
  assert(conn->state() == ConnState::kOpen);

  std::printf("      -> Connection initialized with kOpen state and matching fd.\n");
}

// ----------------------------------------------------------------------------
// 2. Immediate Fast-Path Write
// ----------------------------------------------------------------------------
void TestConnectionImmediateWrite() {
  std::printf("  [2] Running TestConnectionImmediateWrite...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  int sv[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
  SetNonBlocking(sv[0]);
  SetNonBlocking(sv[1]);
  UniqueFd peer_fd(sv[1]);

  auto conn = std::make_shared<Connection>(UniqueFd(sv[0]), loop);

  // Send payload via fast-path (write_buf_ empty)
  std::string message = "HELLO_REACTOR_WORLD";
  conn->Write({reinterpret_cast<const std::byte *>(message.data()), message.size()});

  // Read immediately from peer socket
  char read_buf[64] = {0};
  ssize_t n = read(peer_fd.get(), read_buf, sizeof(read_buf));
  assert(n == static_cast<ssize_t>(message.size()));
  assert(std::string(read_buf, n) == message);

  std::printf("      -> Immediate fast-path write delivered %zd bytes faithfully.\n", n);
}

// ----------------------------------------------------------------------------
// 3. Buffered Inbound Read & Protocol Framing (PING -> ECHO_OK)
// ----------------------------------------------------------------------------
void TestConnectionPingProtocol() {
  std::printf("  [3] Running TestConnectionPingProtocol...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  int sv[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
  SetNonBlocking(sv[0]);
  SetNonBlocking(sv[1]);
  UniqueFd peer_fd(sv[1]);

  auto conn = std::make_shared<Connection>(UniqueFd(sv[0]), loop);

  bool got_echo_ok = false;
  std::string received_buf;

  // Register peer_fd as readable on loop so it wakes when response is sent
  bool ok1 = loop.RegisterFdEvent(peer_fd.get(), IoEvent::kReadable, [&](int fd, IoEvent) {
    char buf[64];
    while (true) {
      ssize_t n = read(fd, buf, sizeof(buf));
      if (n > 0) {
        received_buf.append(buf, n);
      } else {
        break;
      }
    }
    if (received_buf.find("ECHO_OK\n") != std::string::npos) {
      got_echo_ok = true;
      loop.RemoveFdEvent(fd);
      loop.Stop();
    }
  });
  assert(ok1);

  // Send PING command delimited by \n
  const char *cmd = "PING\n";
  assert(write(peer_fd.get(), cmd, std::strlen(cmd)) == static_cast<ssize_t>(std::strlen(cmd)));

  loop.Run();

  assert(got_echo_ok);
  std::printf("      -> Protocol framing successfully dispatched PING -> ECHO_OK.\n");
}

// ----------------------------------------------------------------------------
// 4. Multi-Chunk Protocol Framing Assembly
// ----------------------------------------------------------------------------
void TestConnectionMultiChunkFraming() {
  std::printf("  [4] Running TestConnectionMultiChunkFraming...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  int sv[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
  SetNonBlocking(sv[0]);
  SetNonBlocking(sv[1]);
  UniqueFd peer_fd(sv[1]);

  auto conn = std::make_shared<Connection>(UniqueFd(sv[0]), loop);

  bool received_response = false;
  std::string response_buf;

  bool ok2 = loop.RegisterFdEvent(peer_fd.get(), IoEvent::kReadable, [&](int fd, IoEvent) {
    char buf[64];
    while (true) {
      ssize_t n = read(fd, buf, sizeof(buf));
      if (n > 0) {
        response_buf.append(buf, n);
      } else {
        break;
      }
    }
    if (response_buf.find("ECHO_OK\n") != std::string::npos) {
      received_response = true;
      loop.RemoveFdEvent(fd);
      loop.Stop();
    }
  });
  assert(ok2);

  // Write "PI" (partial chunk 1)
  assert(write(peer_fd.get(), "PI", 2) == 2);

  // Deliver 2nd chunk after 5ms via timer
  loop.RegisterTimerEvent(5ms, [&]() {
    assert(write(peer_fd.get(), "NG\n", 3) == 3);
  });

  loop.Run();

  assert(received_response);
  std::printf("      -> Split chunks [\"PI\" + \"NG\\n\"] assembled properly across reads.\n");
}

// ----------------------------------------------------------------------------
// 5. Peer Disconnect (EOF) Triggers Close & on_close Callback
// ----------------------------------------------------------------------------
void TestConnectionPeerDisconnectEof() {
  std::printf("  [5] Running TestConnectionPeerDisconnectEof...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  int sv[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
  SetNonBlocking(sv[0]);
  SetNonBlocking(sv[1]);

  int conn_fd = sv[0];
  auto conn = std::make_shared<Connection>(UniqueFd(sv[0]), loop);

  bool close_callback_fired = false;
  int closed_fd_reported = -1;

  conn->SetCloseCallback([&](int fd) {
    close_callback_fired = true;
    closed_fd_reported = fd;
    loop.Stop();
  });

  // Peer abruptly closes the other end of the socketpair -> kernel generates EOF
  close(sv[1]);

  loop.Run();

  assert(close_callback_fired);
  assert(closed_fd_reported == conn_fd);
  assert(conn->state() == ConnState::kClosed);
  std::printf("      -> Peer EOF detected, connection closed, on_close invoked.\n");
}

// ----------------------------------------------------------------------------
// 6. Backpressure Buffering & OnWritable Drain
// ----------------------------------------------------------------------------
void TestConnectionBackpressureAndOnWritable() {
  std::printf("  [6] Running TestConnectionBackpressureAndOnWritable...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  int sv[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
  SetNonBlocking(sv[0]);
  SetNonBlocking(sv[1]);
  UniqueFd peer_fd(sv[1]);

  // Set tiny socket send buffer to induce backpressure
  int sndbuf = 2048;
  setsockopt(sv[0], SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

  auto conn = std::make_shared<Connection>(UniqueFd(sv[0]), loop);

  // Create a large payload that exceeds the socket buffer (e.g. 64 KB)
  constexpr size_t kLargeSize = 64 * 1024;
  std::vector<char> large_payload(kLargeSize, 'X');
  for (size_t i = 0; i < kLargeSize; ++i) {
    large_payload[i] = static_cast<char>('A' + (i % 26));
  }

  // Write large buffer; partial write will leave remainder in write_buf_
  // and arm kWritable on conn's fd
  conn->Write({reinterpret_cast<const std::byte *>(large_payload.data()), large_payload.size()});

  // Purely event-driven: peer_fd is registered for kReadable on the event loop.
  // Whenever data is sent or drained, loop invokes this callback.
  std::vector<char> received_payload;
  received_payload.reserve(kLargeSize);

  bool ok3 = loop.RegisterFdEvent(peer_fd.get(), IoEvent::kReadable, [&](int fd, IoEvent) {
    char chunk[4096];
    while (true) {
      ssize_t n = read(fd, chunk, sizeof(chunk));
      if (n > 0) {
        received_payload.insert(received_payload.end(), chunk, chunk + n);
      } else {
        break;
      }
    }
    if (received_payload.size() >= kLargeSize) {
      loop.RemoveFdEvent(fd);
      loop.Stop();
    }
  });
  assert(ok3);

  loop.Run();

  assert(received_payload.size() == kLargeSize);
  assert(std::memcmp(received_payload.data(), large_payload.data(), kLargeSize) == 0);
  std::printf("      -> %zu bytes streamed through backpressure buffer and drained cleanly.\n",
              kLargeSize);
}

// ----------------------------------------------------------------------------
// 7. Half-Closed State with Pending Write Flush
// ----------------------------------------------------------------------------
void TestConnectionHalfClosed() {
  std::printf("  [7] Running TestConnectionHalfClosed...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  int sv[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
  SetNonBlocking(sv[0]);
  SetNonBlocking(sv[1]);
  UniqueFd peer_fd(sv[1]);

  int sndbuf = 2048;
  setsockopt(sv[0], SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

  auto conn = std::make_shared<Connection>(UniqueFd(sv[0]), loop);

  std::vector<char> payload(32 * 1024, 'Z');
  conn->Write({reinterpret_cast<const std::byte *>(payload.data()), payload.size()});

  // Request half-close: must NOT drop pending data immediately!
  conn->Close(/*flush_pending=*/true);
  assert(conn->state() == ConnState::kHalfClosed);

  // Drain peer socket in event loop
  std::vector<char> drained;
  drained.reserve(payload.size());

  bool ok4 = loop.RegisterFdEvent(peer_fd.get(), IoEvent::kReadable, [&](int fd, IoEvent) {
    char buf[4096];
    while (true) {
      ssize_t n = read(fd, buf, sizeof(buf));
      if (n > 0) {
        drained.insert(drained.end(), buf, buf + n);
      } else {
        break;
      }
    }
    if (drained.size() >= payload.size()) {
      loop.RemoveFdEvent(fd);
      loop.Stop();
    }
  });
  assert(ok4);

  loop.Run();

  assert(drained.size() == payload.size());
  assert(conn->state() == ConnState::kClosed);
  std::printf("      -> kHalfClosed preserved pending writes until flushed to kClosed.\n");
}

// ----------------------------------------------------------------------------
// 8. Explicit Close Cleanup
// ----------------------------------------------------------------------------
void TestConnectionExplicitClose() {
  std::printf("  [8] Running TestConnectionExplicitClose...\n");

  ThreadPool pool(1);
  EventLoop loop(pool);

  int sv[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
  SetNonBlocking(sv[0]);
  SetNonBlocking(sv[1]);
  UniqueFd peer_fd(sv[1]);

  auto conn = std::make_shared<Connection>(UniqueFd(sv[0]), loop);

  bool closed = false;
  conn->SetCloseCallback([&](int) { closed = true; });

  conn->Close(/*flush_pending=*/false);
  assert(conn->state() == ConnState::kClosed);
  assert(closed == true);

  // Subsequent writes should be no-ops
  std::string ignored = "IGNORED";
  conn->Write({reinterpret_cast<const std::byte *>(ignored.data()), ignored.size()});

  char buf[16];
  ssize_t n = read(peer_fd.get(), buf, sizeof(buf));
  assert(n <= 0); // Nothing written

  std::printf("      -> Explicit Close immediately shuts down socket & rejects new writes.\n");
}

int main() {
  std::puts("=== mini_reactor Connection Comprehensive Test Suite ===");
  TestConnectionInitialState();
  TestConnectionImmediateWrite();
  TestConnectionPingProtocol();
  TestConnectionMultiChunkFraming();
  TestConnectionPeerDisconnectEof();
  TestConnectionBackpressureAndOnWritable();
  TestConnectionHalfClosed();
  TestConnectionExplicitClose();
  std::puts("=== All 8 Connection unit tests passed successfully! ===\n");
  return 0;
}
