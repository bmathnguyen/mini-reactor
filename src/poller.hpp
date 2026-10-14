#pragma once // only include this header file once per translation unit, no matter how many times it gets #included.

#if defined(__linux__)
  #include "poller/poller_epoll.hpp"
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
  #include "poller/poller_kqueue.hpp"
#else
  #error "mini-reactor: unsupported platform"
#endif