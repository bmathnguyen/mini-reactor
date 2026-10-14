# mini_reactor Technical Documentation Index

Welcome to the internal engineering documentation for `mini_reactor`. This directory contains deep-dive design documents, architectural diagrams, and comparative references against industry systems (Redis, libuv, Envoy).

---

## Documentation Roadmap

```
docs/
├── README.md               # This directory index
├── architecture.md         # Core design decisions, class skeletons, and comparative source maps
├── threading_model.md      # Thread count math (1 + N + M), inventory, and ReactorThread lifecycle
├── multi_reactor_flow.md   # SO_REUSEPORT load balancing and share-nothing multi-reactor flow
└── request_flow.md         # Request sequence diagrams, connection state machine, fast vs slow path
```

---

## Topic Guide

### 1. [System Architecture & Design Rationale](file:///Users/bmathnguyen/Documents/Code/Cpp-Codebase/event-loop/docs/architecture.md)
* **What you'll learn**:
  - The 2-Tier Concurrency Model ($1 + N + M$).
  - Source-level comparison against **Redis (`ae.c`)**, **Node.js (`libuv`)**, and **Envoy Proxy**.
  - Why we chose flat callback vectors over `std::unordered_map`.
  - Why timers use min-heaps with lazy cancellation.
  - OS Poller backend abstraction (`epoll` on Linux vs `kqueue` on macOS/BSD).

### 2. [Threading Model & Thread Inventory](file:///Users/bmathnguyen/Documents/Code/Cpp-Codebase/event-loop/docs/threading_model.md)
* **What you'll learn**:
  - The thread formula: $\text{Total Threads} = 1 \text{ (Main)} + N \text{ (Reactors)} + M \text{ (Workers)}$.
  - Why **10,000 active clients require ZERO client threads**.
  - Memory analysis: Thread-per-connection (80 GB stack overhead) vs. Reactor (a few KB per client).
  - Why reactor threads are encapsulated into the [`ReactorThread`](file:///Users/bmathnguyen/Documents/Code/Cpp-Codebase/event-loop/include/mini_reactor/reactor_thread.hpp) RAII class.

### 3. [Multi-Reactor Flow & `SO_REUSEPORT`](file:///Users/bmathnguyen/Documents/Code/Cpp-Codebase/event-loop/docs/multi_reactor_flow.md)
* **What you'll learn**:
  - Why a single event loop bottlenecks on multi-core servers.
  - How Linux kernel `SO_REUSEPORT` distributes incoming connections with zero user-space locking.
  - Share-nothing thread isolation: cache locality and lock-free connection buffers.
  - Cross-thread wakeup handoff using `wakeup_fd_` (`eventfd`).

### 4. [Request Flow & State Machine](file:///Users/bmathnguyen/Documents/Code/Cpp-Codebase/event-loop/docs/request_flow.md)
* **What you'll learn**:
  - Complete Mermaid sequence diagrams tracing a request through kernel, reactor, and worker threads.
  - **Connection State Machine**: Transitions between `kOpen`, `kHalfClosed` (backpressure draining), and `kClosed`.
  - Fast-Path (direct in-place echo) vs. Slow-Path (thread pool offloading) performance comparison.
