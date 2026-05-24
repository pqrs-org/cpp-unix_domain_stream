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

  // Interval used by the server to verify that its socket path is still
  // connectable and that the accepted connection can exchange an internal
  // health-check frame.
  // Set to std::nullopt to disable server health checks.
  std::optional<std::chrono::milliseconds> server_check_interval = std::nullopt;

  // Maximum time allowed for one server health check.
  std::chrono::milliseconds server_check_timeout = std::chrono::milliseconds(1000);

  // Interval used to send heartbeat frames to the peer.
  // Set to std::nullopt to disable heartbeat sending.
  std::optional<std::chrono::milliseconds> heartbeat_interval = std::nullopt;

  // Maximum idle time allowed without receiving any frame from the peer.
  // Heartbeat, health-check and user-data frames all refresh this deadline.
  // Set to std::nullopt to disable heartbeat timeout checks.
  std::optional<std::chrono::milliseconds> heartbeat_timeout = std::nullopt;

  // Maximum time allowed for one async write operation.
  // Set to std::nullopt to disable write timeouts.
  std::optional<std::chrono::milliseconds> write_timeout = std::chrono::milliseconds(5000);
};

} // namespace pqrs::unix_domain_stream
