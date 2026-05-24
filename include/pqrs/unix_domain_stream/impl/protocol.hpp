#pragma once

// (C) Copyright Takayama Fumihiko 2026.
// Distributed under the Boost Software License, Version 1.0.
// (See https://www.boost.org/LICENSE_1_0.txt)

#include <array>
#include <cstdint>
#include <cstring>
#include <vector>

namespace pqrs::unix_domain_stream::impl::protocol {

enum class message_type : uint8_t {
  heartbeat,
  user_data,
  health_check,
  health_check_response,
};

constexpr size_t header_size = sizeof(uint32_t);
constexpr size_t type_size = sizeof(uint8_t);

inline void encode_uint32(std::array<uint8_t, header_size>& output,
                          uint32_t value) {
  output[0] = static_cast<uint8_t>((value >> 24) & 0xff);
  output[1] = static_cast<uint8_t>((value >> 16) & 0xff);
  output[2] = static_cast<uint8_t>((value >> 8) & 0xff);
  output[3] = static_cast<uint8_t>(value & 0xff);
}

inline uint32_t decode_uint32(const std::array<uint8_t, header_size>& input) {
  return (static_cast<uint32_t>(input[0]) << 24) |
         (static_cast<uint32_t>(input[1]) << 16) |
         (static_cast<uint32_t>(input[2]) << 8) |
         static_cast<uint32_t>(input[3]);
}

inline std::vector<uint8_t> make_frame(message_type type,
                                       const uint8_t* data,
                                       size_t size) {
  auto body_size = type_size + size;

  std::array<uint8_t, header_size> header;
  encode_uint32(header, static_cast<uint32_t>(body_size));

  std::vector<uint8_t> frame;
  frame.reserve(header_size + body_size);
  frame.insert(frame.end(), header.begin(), header.end());
  frame.push_back(static_cast<uint8_t>(type));

  if (data && size > 0) {
    frame.insert(frame.end(), data, data + size);
  }

  return frame;
}

inline std::vector<uint8_t> make_user_data_frame(const std::vector<uint8_t>& data) {
  return make_frame(message_type::user_data,
                    data.data(),
                    data.size());
}

inline std::vector<uint8_t> make_heartbeat_frame() {
  return make_frame(message_type::heartbeat,
                    nullptr,
                    0);
}

inline std::vector<uint8_t> make_health_check_frame() {
  return make_frame(message_type::health_check,
                    nullptr,
                    0);
}

inline std::vector<uint8_t> make_health_check_response_frame() {
  return make_frame(message_type::health_check_response,
                    nullptr,
                    0);
}

} // namespace pqrs::unix_domain_stream::impl::protocol
