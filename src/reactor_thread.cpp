#include "mini_reactor/reactor_thread.hpp"
#include "fd_guard.hpp"
#include "mini_reactor/connection.hpp"
#include "mini_reactor/event_loop.hpp"
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

ReactorThread::ReactorThread(uint16_t port, size_t reactor_idx,
                             ThreadPool &global_thread_pool)
    : port_(port), reactor_idx_(reactor_idx),
      global_thread_pool_(global_thread_pool), loop_(global_thread_pool) {}

ReactorThread::~ReactorThread() { Stop(); }

void ReactorThread::ThreadMain() {
  SetupClientListeningSocket();
  loop_.Run();
}

static void SetNonBlocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void ReactorThread::SetupClientListeningSocket() {
  listen_fd_ = UniqueFd(socket(AF_INET, SOCK_STREAM, 0));
  SetNonBlocking(listen_fd_.get());
  int opt = 1;
  setsockopt(listen_fd_.get(), SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port_);
  bind(listen_fd_.get(), (sockaddr *)&addr, sizeof(addr));
  listen(listen_fd_.get(), SOMAXCONN);
  (void)loop_.RegisterFdEvent(
      listen_fd_.get(), IoEvent::kReadable, [this](int, IoEvent) {
        while (true) {
          sockaddr_in client_addr{};
          socklen_t client_len = sizeof(client_addr);
          int client_sock =
              ::accept(listen_fd_.get(), (sockaddr *)&client_addr, &client_len);
          if (client_sock < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
              break;
            if (errno == EINTR)
              continue;
            break;
          }
          SetNonBlocking(client_sock);
          auto conn =
              std::make_shared<Connection>(UniqueFd(client_sock), loop_);
          conn->SetCloseCallback([this](int fd) {
            active_connections_.erase(fd);
            connection_count_.fetch_sub(1, std::memory_order_relaxed);
          });
          active_connections_[client_sock] = conn;
          connection_count_.fetch_add(1, std::memory_order_relaxed);
        }
      });
}

void ReactorThread::Start() {
  thread_ = std::jthread(&ReactorThread::ThreadMain, this);
}

void ReactorThread::Stop() { loop_.Stop(); }