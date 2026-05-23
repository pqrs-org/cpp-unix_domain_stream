#pragma once

// (C) Copyright Takayama Fumihiko 2026.
// Distributed under the Boost Software License, Version 1.0.
// (See https://www.boost.org/LICENSE_1_0.txt)

#include <chrono>
#include <cstddef>
#include <optional>

namespace pqrs::unix_domain_stream {

struct options final {
  // Soft limit for one application message payload.
  // This prevents excessive memory use when sending or receiving unexpectedly large frames.
  size_t max_message_size = 32 * 1024;

  // Maximum number of unsent frames kept in the per-peer write queue.
  // This limits memory growth when the peer is slow or the caller sends faster
  // than the socket can write.
  size_t max_send_queue_size = 1024;

  // Interval used to retry client connect or server bind after a failure.
  // Set to std::nullopt to disable automatic reconnect/rebind attempts.
  std::optional<std::chrono::milliseconds> reconnect_interval = std::chrono::milliseconds(1000);

  // Maximum time allowed for one async write operation.
  // Set to std::nullopt to disable write timeouts.
  std::optional<std::chrono::milliseconds> write_timeout = std::chrono::milliseconds(5000);
};

} // namespace pqrs::unix_domain_stream
