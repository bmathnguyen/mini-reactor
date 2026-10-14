#pragma once

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

#include "fd_guard.hpp"

class EventLoop;
// halfClosed mean: waiting to flush the rest of write data before closing
enum class ConnState { kOpen, kHalfClosed, kClosed };
using CloseCallBack = std::function<void(int fd)>;

// enable_shared_from_this will internally keep track of the original
// control_block so that when we create new weak_ptr/shared_ptr, they will
// use the same control_block!
class Connection : public std::enable_shared_from_this<Connection> {
public:
  Connection(UniqueFd fd, EventLoop &loop); // loop reference: never null
  ~Connection();

  Connection(const Connection &) = delete;
  Connection &operator=(const Connection &) = delete;

  void OnReadable(); // read() client request in a loop, append to read_buf_
  void OnWritable(); // flush write_buf_ to fd
  void Write(std::span<const std::byte> data); // response to client
  void Close(bool flush_pending);
  void SetCloseCallback(CloseCallBack cb) { on_close_ = std::move(cb); }

  [[nodiscard]] ConnState state() const noexcept { return state_; }
  [[nodiscard]] int fd() const noexcept { return fd_.get(); }

private:
  UniqueFd fd_;
  EventLoop &loop_;
  std::vector<std::byte> read_buf_;
  std::vector<std::byte> write_buf_;
  ConnState state_ = ConnState::kOpen;
  int read_disable_count_ = 0; // mirrors Envoy's readDisable refcount
  CloseCallBack on_close_;
  void ParseAndProcessBufferedRequests();
  void ProcessMockCommand(std::string_view &cmd);
};