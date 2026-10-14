cmake_minimum_required(VERSION 3.20)
project(mini_reactor VERSION 1.0.0 LANGUAGES CXX)

# Enforce modern C++20 standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler optimization & warnings
if(MSVC)
  add_compile_options(/W4 /permissive-)
else()
  add_compile_options(-Wall -Wextra -Wpedantic -O2)
  if(APPLE)
    # AppleClang guards std::jthread and std::stop_token behind experimental library flag
    add_compile_options(-fexperimental-library)
  endif()
endif()

# Find POSIX Threads (cross-platform pthread support)
find_package(Threads REQUIRED)

# -------------------------------------------------------------
# 1. Main Static Library (mini_reactor)
# -------------------------------------------------------------
set(LIB_SOURCES
    src/connection.cpp
    src/event_loop.cpp
    src/log.cpp
    src/reactor_thread.cpp
    src/server.cpp
    src/thread_pool.cpp
)

# Platform-conditional OS Poller backend
if(APPLE)
  list(APPEND LIB_SOURCES src/poller/poller_kqueue.cpp)
elseif(UNIX)
  list(APPEND LIB_SOURCES src/poller/poller_epoll.cpp)
endif()

add_library(mini_reactor STATIC ${LIB_SOURCES})

# Best Practice Target-Based Include Directories:
# - PUBLIC: Exported to consumers linking with mini_reactor
# - PRIVATE: Internal headers inside src/ (not visible to consumers)
target_include_directories(mini_reactor
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:include>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(mini_reactor PUBLIC Threads::Threads)

# -------------------------------------------------------------
# 2. Examples
# -------------------------------------------------------------
add_executable(echo_server examples/echo_server.cpp)
target_link_libraries(echo_server PRIVATE mini_reactor)

add_executable(high_level_server examples/high_level_server.cpp)
target_link_libraries(high_level_server PRIVATE mini_reactor)

# -------------------------------------------------------------
# 3. Unit Tests
# -------------------------------------------------------------
enable_testing()

add_executable(event_loop_test tests/event_loop_test.cpp)
target_link_libraries(event_loop_test PRIVATE mini_reactor)
add_test(NAME EventLoopTest COMMAND event_loop_test)

add_executable(connection_test tests/connection_test.cpp)
target_link_libraries(connection_test PRIVATE mini_reactor)
add_test(NAME ConnectionTest COMMAND connection_test)

add_executable(thread_pool_test tests/thread_pool_test.cpp)
target_link_libraries(thread_pool_test PRIVATE mini_reactor)
add_test(NAME ThreadPoolTest COMMAND thread_pool_test)

add_executable(reactor_thread_test tests/reactor_thread_test.cpp)
target_link_libraries(reactor_thread_test PRIVATE mini_reactor)
add_test(NAME ReactorThreadTest COMMAND reactor_thread_test)

add_executable(log_test tests/log_test.cpp)
target_link_libraries(log_test PRIVATE mini_reactor)
add_test(NAME LogTest COMMAND log_test)
