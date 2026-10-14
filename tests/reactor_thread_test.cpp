#include "mini_reactor/reactor_thread.hpp"
#include "mini_reactor/thread_pool.hpp"

#include <arpa/inet.h>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;

static int ConnectTcpClient(uint16_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  assert(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1);

  for (int attempt = 0; attempt < 20; ++attempt) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock >= 0) {
      if (::connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0) {
        return sock;
      }
      close(sock);
    }
    std::this_thread::sleep_for(15ms);
  }
  return -1;
}

// ----------------------------------------------------------------------------
// 1. ReactorThread Startup, Accessors, and Clean Shutdown
// ----------------------------------------------------------------------------
void TestReactorThreadLifecycle() {
  std::printf("  [1] Running TestReactorThreadLifecycle...\n");

  ThreadPool pool(1);
  constexpr uint16_t kPort = 19871;

  ReactorThread reactor(kPort, 0, pool);
  assert(reactor.reactor_idx() == 0);
  assert(reactor.connection_count() == 0);

  reactor.Start();
  std::this_thread::sleep_for(30ms);

  // Stop should request loop stop and join jthread cleanly
  reactor.Stop();
  std::printf("      -> ReactorThread spawned, queried, and joined cleanly.\n");
}

// ----------------------------------------------------------------------------
// 2. Real TCP Client Connect, Framing Dispatch, and Disconnect Accounting
// ----------------------------------------------------------------------------
void TestReactorThreadClientConnectAndEcho() {
  std::printf("  [2] Running TestReactorThreadClientConnectAndEcho...\n");

  ThreadPool pool(1);
  constexpr uint16_t kPort = 19872;

  ReactorThread reactor(kPort, 1, pool);
  reactor.Start();

  int client_fd = ConnectTcpClient(kPort);
  assert(client_fd >= 0);

  // Allow accept callback to run
  for (int i = 0; i < 50; ++i) {
    if (reactor.connection_count() == 1) break;
    std::this_thread::sleep_for(5ms);
  }
  assert(reactor.connection_count() == 1);

  // Send PING command over TCP
  const char *msg = "PING\n";
  assert(write(client_fd, msg, std::strlen(msg)) == static_cast<ssize_t>(std::strlen(msg)));

  // Read response
  char buf[32] = {0};
  ssize_t n = read(client_fd, buf, sizeof(buf) - 1);
  assert(n > 0);
  assert(std::string(buf, n) == "ECHO_OK\n");

  // Close client TCP connection
  close(client_fd);

  // Allow EOF callback to clean up active connection
  for (int i = 0; i < 50; ++i) {
    if (reactor.connection_count() == 0) break;
    std::this_thread::sleep_for(5ms);
  }
  assert(reactor.connection_count() == 0);

  reactor.Stop();
  std::printf("      -> TCP client connection, PING/ECHO_OK exchange, and EOF cleanup verified.\n");
}

// ----------------------------------------------------------------------------
// 3. Multiple Concurrent TCP Clients on Single Reactor
// ----------------------------------------------------------------------------
void TestReactorThreadMultipleConcurrentClients() {
  std::printf("  [3] Running TestReactorThreadMultipleConcurrentClients...\n");

  ThreadPool pool(1);
  constexpr uint16_t kPort = 19873;
  constexpr int kNumClients = 5;

  ReactorThread reactor(kPort, 2, pool);
  reactor.Start();

  std::vector<int> client_fds;
  for (int i = 0; i < kNumClients; ++i) {
    int fd = ConnectTcpClient(kPort);
    assert(fd >= 0);
    client_fds.push_back(fd);
  }

  // Verify all connections accepted
  for (int i = 0; i < 50; ++i) {
    if (reactor.connection_count() == kNumClients) break;
    std::this_thread::sleep_for(5ms);
  }
  assert(reactor.connection_count() == kNumClients);

  // Send PING on each client
  for (int fd : client_fds) {
    assert(write(fd, "PING\n", 5) == 5);
  }

  // Read ECHO_OK from each client
  for (int fd : client_fds) {
    char buf[32] = {0};
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    assert(n > 0);
    assert(std::string(buf, n) == "ECHO_OK\n");
  }

  // Disconnect all
  for (int fd : client_fds) {
    close(fd);
  }

  for (int i = 0; i < 50; ++i) {
    if (reactor.connection_count() == 0) break;
    std::this_thread::sleep_for(5ms);
  }
  assert(reactor.connection_count() == 0);

  reactor.Stop();
  std::printf("      -> %d concurrent TCP clients served concurrently without contention.\n",
              kNumClients);
}

int main() {
  std::puts("=== mini_reactor ReactorThread Comprehensive Test Suite ===");
  TestReactorThreadLifecycle();
  TestReactorThreadClientConnectAndEcho();
  TestReactorThreadMultipleConcurrentClients();
  std::puts("=== All 3 ReactorThread unit tests passed successfully! ===\n");
  return 0;
}
