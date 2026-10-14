#include "mini_reactor/log.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace mini_reactor;

// ----------------------------------------------------------------------------
// Test 1: Log Level Filtering (Zero-Cost Bypass)
// ----------------------------------------------------------------------------
void TestLogLevelFiltering() {
  std::printf("  [1] Running TestLogLevelFiltering...\n");

  std::vector<LogRecord> captured;
  Log::SetSink([&](const LogRecord& record) {
    captured.push_back(record);
  });

  // Filter set to kWarn: Trace, Debug, Info must be dropped
  Log::SetLevel(LogLevel::kWarn);
  assert(Log::GetLevel() == LogLevel::kWarn);

  Log::Trace("This trace should be ignored");
  Log::Debug("This debug should be ignored");
  Log::Info("This info should be ignored");
  assert(captured.empty());

  // Warn and Error must pass through
  Log::Warn("Warning message: {}", 42);
  assert(captured.size() == 1);
  assert(captured[0].level == LogLevel::kWarn);
  assert(captured[0].message == "Warning message: 42");

  Log::Error("Error code: {}", "ECONNRESET");
  assert(captured.size() == 2);
  assert(captured[1].level == LogLevel::kError);
  assert(captured[1].message == "Error code: ECONNRESET");

  // Setting level to kOff must drop even Critical
  Log::SetLevel(LogLevel::kOff);
  Log::Critical("System failure!");
  assert(captured.size() == 2);

  Log::ResetSink();
  std::printf("      Passed TestLogLevelFiltering.\n");
}

// ----------------------------------------------------------------------------
// Test 2: Formatting and Type Support
// ----------------------------------------------------------------------------
void TestFormattingAndTypes() {
  std::printf("  [2] Running TestFormattingAndTypes...\n");

  LogRecord last_record{};
  Log::SetSink([&](const LogRecord& record) {
    last_record = record;
  });

  Log::SetLevel(LogLevel::kDebug);

  Log::Debug("Integer: {}, Float: {:.2f}, String: {}", 123, 3.14159, "hello");
  assert(last_record.level == LogLevel::kDebug);
  assert(last_record.message == "Integer: 123, Float: 3.14, String: hello");

  Log::ResetSink();
  std::printf("      Passed TestFormattingAndTypes.\n");
}

// ----------------------------------------------------------------------------
// Test 3: Source Location Tracking
// ----------------------------------------------------------------------------
void TestSourceLocation() {
  std::printf("  [3] Running TestSourceLocation...\n");

  LogRecord captured{};
  Log::SetSink([&](const LogRecord& record) {
    captured = record;
  });

  Log::SetLevel(LogLevel::kInfo);

  // Line marker test: record line number exactly
  int expected_line = __LINE__ + 1;
  Log::Info("Testing source location capture");

  assert(captured.location.line() == static_cast<uint32_t>(expected_line));
  std::string_view file_name = captured.location.file_name();
  assert(file_name.find("log_test.cpp") != std::string_view::npos);

  Log::ResetSink();
  std::printf("      Passed TestSourceLocation.\n");
}

// ----------------------------------------------------------------------------
// Test 4: Thread-Safe Concurrent Invocations
// ----------------------------------------------------------------------------
void TestConcurrentLogging() {
  std::printf("  [4] Running TestConcurrentLogging...\n");

  std::atomic<size_t> counter{0};
  Log::SetSink([&](const LogRecord& /*record*/) {
    counter.fetch_add(1, std::memory_order_relaxed);
  });

  Log::SetLevel(LogLevel::kTrace);

  constexpr size_t kNumThreads = 4;
  constexpr size_t kLogsPerThread = 1000;
  std::vector<std::jthread> threads;
  threads.reserve(kNumThreads);

  for (size_t t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([t]() {
      for (size_t i = 0; i < kLogsPerThread; ++i) {
        Log::Info("Thread {} logging iteration {}", t, i);
      }
    });
  }

  // Join all threads
  threads.clear();

  assert(counter.load() == kNumThreads * kLogsPerThread);

  Log::ResetSink();
  std::printf("      Passed TestConcurrentLogging (received %zu logs).\n", counter.load());
}

// ----------------------------------------------------------------------------
// Test 5: LogLevel String Conversion
// ----------------------------------------------------------------------------
void TestLogLevelStrings() {
  std::printf("  [5] Running TestLogLevelStrings...\n");

  assert(ToString(LogLevel::kTrace) == "TRACE");
  assert(ToString(LogLevel::kDebug) == "DEBUG");
  assert(ToString(LogLevel::kInfo) == "INFO");
  assert(ToString(LogLevel::kWarn) == "WARN");
  assert(ToString(LogLevel::kError) == "ERROR");
  assert(ToString(LogLevel::kCritical) == "CRITICAL");
  assert(ToString(LogLevel::kOff) == "OFF");

  std::printf("      Passed TestLogLevelStrings.\n");
}

int main() {
  std::printf("=== Starting Log Subsystem Tests ===\n");

  TestLogLevelFiltering();
  TestFormattingAndTypes();
  TestSourceLocation();
  TestConcurrentLogging();
  TestLogLevelStrings();

  std::printf("=== All Log Subsystem Tests Passed Successfully ===\n");
  return 0;
}
