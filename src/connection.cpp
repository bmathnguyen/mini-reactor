#include "mini_reactor/connection.hpp"

#include "mini_reactor/event_loop.hpp"
#include "mini_reactor/thread_pool.hpp"
#include <iterator>

Connection::Connection(UniqueFd fd, EventLoop &loop)
    : fd_(std::move(fd)), loop_(loop) {
  // Register fd event to Event Loop to get notified on kReadable
  // when it's ready: will callback (3rd arg)
  if (!loop_.RegisterFdEvent(fd_.get(), IoEvent::kReadable,
                             [this](int, IoEvent mask) {
                               if ((mask & IoEvent::kReadable) != IoEvent(0)) {
                                 this->OnReadable();
                               }
                               if ((mask & IoEvent::kWritable) != IoEvent(0)) {
                                 // NOTE: since we will MODIFY later on
                                 // kReadable when kernel write buffer is full
                                 // (which will trigger EPOLLOUT)!
                                 this->OnWritable();
                               }
                             })) {
    state_ = ConnState::kClosed;
  }
}

Connection::~Connection() {
  // close connection (will de-register fd, flush pending write buffer if
  // needed)
  Close(false);
}

void Connection::OnReadable() {
  // >0: got data, ==0: peer closed, <0: EAGAIN vs. real error
  std::byte buffer[4096]; // same as unsigned char
  // read from fd to buffer until EOF
  while (true) {
    ssize_t bytes_read = read(fd_.get(), buffer, sizeof(buffer));
    if (bytes_read > 0) {
      read_buf_.insert(read_buf_.end(), buffer, buffer + bytes_read);
    } else if (bytes_read == 0) {
      // EOF, peer closed
      Close(false);
      break;
    } else {
      // should save errno before calling to any other function that may
      // overwrite it! E.g. every libc wrapper overwrite errno
      errno_t err = errno;
      if (err == EAGAIN || err == EWOULDBLOCK) {
        // No data available anymore, "would block", break the loop
        break;
      } else if (err == EINTR) {
        // interrupted by signal, try reading again
        // read() blocks ──► signal arrives ──► handler runs ──► read() returns
        // -1, errno=EINTR
        continue;
      } else {
        // Fatal error, close connection
        Close(false);
        return;
      }
    }
  }
  if (read_buf_.empty()) {
    // no data in buffer
    return;
  }
  ParseAndProcessBufferedRequests();
}

void Connection::ParseAndProcessBufferedRequests() {
  // NOTE: Subject to changes depending on the protocol specification.
  // find the first '\n'
  auto iterator =
      std::find(read_buf_.begin(), read_buf_.end(), std::byte('\n'));
  while (iterator != read_buf_.end()) {
    // std::distance(it1, it2) returns std::ptrdiff_t
    auto command =
        std::string_view(reinterpret_cast<const char *>(read_buf_.data()),
                         std::distance(read_buf_.begin(), iterator));
    ProcessMockCommand(command);
    // continue with the next command in the buffer
    read_buf_.erase(read_buf_.begin(), iterator + 1); // [begin, end)
    // find the next '\n' to process
    iterator = std::find(read_buf_.begin(), read_buf_.end(), std::byte('\n'));
  }
}

void Connection::ProcessMockCommand(std::string_view &cmd) {
  // FAST PATH: no work
  if (cmd == "PING") {
    std::string resp = "ECHO_OK\n";
    Write({reinterpret_cast<const std::byte *>(resp.data()), resp.size()});
    return;
  }
  // SLOW PATH: use thread pool
  WorkKind work_kind = WorkKind::kCpu;
  std::string response;
  if (cmd == "CPU") {
    work_kind = WorkKind::kCpu;
  } else if (cmd == "FAST_IO") {
    work_kind = WorkKind::kFastIo;
  } else if (cmd == "SLOW_IO") {
    work_kind = WorkKind::kSlowIo;
  }
  auto shared_result = std::make_shared<std::string>();
  // MOCK: simulate the work
  auto work = [work_kind, shared_result]() {
    // capture 'work_kind' by value (copy)
    if (work_kind == WorkKind::kCpu) {
      // Simulate a heavy CPU task blocking the core for 500ms
      auto start = std::chrono::steady_clock::now();
      while (std::chrono::steady_clock::now() - start <
             std::chrono::milliseconds(500)) {
        // Busy wait...
      }
      *shared_result = "CPU_OK\n";
    } else if (work_kind == WorkKind::kSlowIo) {
      // Simulate a slow database or DNS query
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      *shared_result = "SLOW_IO_OK\n";
    } else if (work_kind == WorkKind::kFastIo) {
      // Simulate a fast SSD read
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      *shared_result = "FAST_IO_OK\n";
    }
  };
  // on_done callback on Reactor Thread: echo back to client
  // NOTE: for async callback: capturing the raw "this" pointer (Connection)
  // for background task is UNSAFE! (the connection may have been dropped)
  // We can fix it by using enable_shared_from_this
  auto weak_self = weak_from_this();
  auto on_done = [weak_self, shared_result]() {
    // try to acquire strong reference to `this` object if it still exists
    if (auto shared_self = weak_self.lock()) {
      shared_self->Write(
          {reinterpret_cast<const std::byte *>(shared_result->data()),
           shared_result->size()}); // ->data() returns T*, .begin() returns
                                    // std::vector<T>::iterator
    }
    // else: client has disconnected, ignore the work
  };
  loop_.GetGlobalThreadPool().SubmitWork(loop_, work, on_done, work_kind);
}

void Connection::OnWritable() {
  // TODO(Phase 2): drain write_buf_ via write(), re-arm EPOLLOUT if partial.
  if (write_buf_.empty()) {
    return;
  }
  // Write as much data to kernel buffer
  ssize_t written_bytes =
      write(fd_.get(), write_buf_.data(), write_buf_.size());
  if (written_bytes > 0) {
    // erase the written data from write_buf_
    write_buf_.erase(write_buf_.begin(), write_buf_.begin() + written_bytes);
  }
  if (write_buf_.empty()) {
    // tells OS to stop tracking writable!
    if (!loop_.ModifyFdEvent(fd_.get(), IoEvent::kReadable)) {
      Close(false);
      return;
    }
    if (state_ == ConnState::kHalfClosed) {
      Close(false);
    }
  } else if (written_bytes < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
    // actual error -> close!
    Close(false);
  }
  // otherwise: partial write, do nothing, keep same register state!
}

void Connection::Write(std::span<const std::byte> data) {
  // write as much as we can, if not empty, register (modify) onWritable to OS!
  if (state_ != ConnState::kOpen)
    return;
  // don't push to buffer yet to save memory allocation!
  ssize_t written_bytes = 0;
  if (write_buf_.empty()) {
    // FAST PATH: no pending data, try to write out immediately
    written_bytes = write(fd_.get(), data.data(), data.size());
    // handle error
    if (written_bytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        written_bytes = 0; // Non-blocking: OS buffer is full. Treat as 0 bytes
                           // written!
      } else {
        Close(false); // Fatal error: close the connection
        return;
      }
    }
  }
  // handle remaining write buffer for both cases
  if (static_cast<size_t>(written_bytes) < data.size()) {
    // need to push remaining data to buffer
    write_buf_.insert(write_buf_.end(), data.begin() + written_bytes,
                      data.end());
    // register for when kernel buffer can be written to
    if (!loop_.ModifyFdEvent(fd_.get(),
                             IoEvent::kWritable | IoEvent::kReadable)) {
      Close(false);
      return;
    }
  }
}

void Connection::Close(bool flush_pending) {
  if (state_ == ConnState::kClosed)
    return;
  if (flush_pending && !write_buf_.empty()) {
    // still some data need to be flushed, half-closed state!
    state_ = ConnState::kHalfClosed;
    return; // epoll onWritable should already be set to notify us!
  }
  state_ = ConnState::kClosed;
  loop_.RemoveFdEvent(fd_.get());
  if (on_close_) {
    on_close_(fd_.get()); // clean up properly!
  }
}
