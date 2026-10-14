# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`mini-reactor` is a modern C++20 library that handles tens of thousands of simultaneous TCP connections using an asynchronous multi-reactor pattern instead of one thread per connection — a miniature, modern C++ version of the engines underneath Redis (`ae.c`), Node's `libuv`, and Envoy.

Engineering documentation, architecture diagrams, and design rationales live in the [`docs/`](docs/README.md) directory:
- [`docs/architecture.md`](docs/architecture.md) — Core design rationale, source comparisons with Redis/libuv/Envoy, and class skeletons.
- [`docs/threading_model.md`](docs/threading_model.md) — 2-tier concurrency breakdown, thread math ($1 + N + M$), and thread inventory.
- [`docs/multi_reactor_flow.md`](docs/multi_reactor_flow.md) — `SO_REUSEPORT` kernel load balancing and share-nothing multi-reactor flow.
- [`docs/request_flow.md`](docs/request_flow.md) — Sequence diagrams, connection state machine, fast vs slow path offloading.

## Current Implementation State

- **`EventLoop`** (`include/mini_reactor/event_loop.hpp`, `src/event_loop.cpp`): Fully implemented with OS poller integration, timer priority queue (min-heap with lazy cancellation), and dynamic callback storage.
- **`Connection`** (`include/mini_reactor/connection.hpp`, `src/connection.cpp`): Fully implemented with state machine (`kOpen`, `kHalfClosed`, `kClosed`), buffered I/O, backpressure `kWritable` re-arming, protocol framing, and EOF handling.
- **`ReactorThread`** (`include/mini_reactor/reactor_thread.hpp`, `src/reactor_thread.cpp`): Fully implemented RAII class owning a `std::jthread`, `SO_REUSEPORT` listening socket, and private `EventLoop`.
- **`Server`** (`include/mini_reactor/server.hpp`, `src/server.cpp`): Orchestrates $N$ reactor threads and a shared thread pool with graceful shutdown.
- **`ThreadPool`** (`include/mini_reactor/thread_pool.hpp`, `src/thread_pool.cpp`): Class skeleton defined; worker execution queue in progress (Phase 5).
- **`Poller`** (`src/poller.hpp`, `src/poller/poller_kqueue.cpp`): Fully working on macOS/BSD; Linux epoll backend scaffolded.

## Build, Test & Reset

The single source of truth for builds is `CMakeLists.txt`. A standardized `Makefile` acts as a clean wrapper:

```sh
make build       # Compile library, examples, and tests via CMake
make test        # Run all test suites via CTest (100% passing)
make run-server  # Launch high-level multi-reactor server example
make run-echo    # Launch low-level echo server example
make clean       # Remove build/ directory
make reset       # Deep clean all caches, object files, and build directories
make re          # Reset and rebuild fresh from scratch
```

Run test suite directly:
```sh
ctest --test-dir build --output-on-failure
```

## Architecture & Code Conventions

1. **Header/Source Split**:
   - `include/mini_reactor/*.hpp`: Public API headers (users include these).
   - `src/*.hpp` (`fd_guard.hpp`, `poller.hpp`): Internal headers not exposed publicly.
   - `src/*.cpp`: Implementation bodies.
2. **Resource Ownership**:
   - All OS file descriptors are owned via RAII [`UniqueFd`](src/fd_guard.hpp). Raw integers are never leaked.
3. **No Mutexes on Connections**:
   - Connections belong permanently to their accepting `ReactorThread`. Connection buffers (`read_buf_`, `write_buf_`) are never touched across threads without going through the loop's `wakeup_fd_`.
4. **Compile-time Checks**:
   - `[[nodiscard]]` is strictly enforced on fallible system and registration calls.
