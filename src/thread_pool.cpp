#include "mini_reactor/thread_pool.hpp"

#include "mini_reactor/event_loop.hpp"

ThreadPool::ThreadPool(size_t n_threads) {
  // TODO(Phase 5): event_fd_ = eventfd(0, EFD_NONBLOCK), register it with
  // loop_ via AddFd(event_fd_.get(), IoEvent::kReadable, ...), then spawn
  // n_threads workers running WorkerLoop.
  (void)n_threads;
}

ThreadPool::~ThreadPool() {
  // TODO(Phase 5): push n_threads sentinel Jobs (empty `work`), notify_all()
  // on queue_cv_, let std::jthread's destructor join (jthread also
  // request_stop()s on destruction, but the sentinel is what actually wakes
  // a worker blocked in the wait).
}

void ThreadPool::SubmitWork(EventLoop &loop, std::function<void()> work,
                            std::function<void()> on_done, WorkKind kind) {
  // TODO(Phase 5): push a Job{work, on_done, kind} onto queue_ under queue_mu_,
  // notify_one() on queue_cv_.
  auto job = Job{
      .work = work,
      .on_done = on_done,
      .eventloop = loop,
      .kind = kind,
  };
  
}

void ThreadPool::WorkerLoop(std::stop_token stop_token) {
  // TODO(Phase 5): wait on queue_cv_ for a Job or a stop request, run the
  // Job's work (or exit on a sentinel / stop_token.stop_requested()), push
  // {on_done} onto results_ under results_mu_, then write() one byte to
  // event_fd_ to wake the loop thread.
  (void)stop_token;
}

void ThreadPool::OnWakeupFd() {
  // TODO(Phase 5): read() the eventfd's 8-byte counter to reset it, then
  // drain results_ under results_mu_ and run each on_done callback here,
  // on loop_'s thread.
}
