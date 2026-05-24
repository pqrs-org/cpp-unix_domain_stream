#pragma once

// (C) Copyright Takayama Fumihiko 2026.
// Distributed under the Boost Software License, Version 1.0.
// (See https://www.boost.org/LICENSE_1_0.txt)

#include "../options.hpp"
#include "asio_helper.hpp"
#include "protocol.hpp"
#include <deque>
#include <nod/nod.hpp>
#include <pqrs/dispatcher.hpp>
#include <pqrs/gsl.hpp>

namespace pqrs::unix_domain_stream::impl {

class peer final : public dispatcher::extra::dispatcher_client,
                   public std::enable_shared_from_this<peer> {
public:
  nod::signal<void()> ready;
  nod::signal<void(not_null_shared_ptr_t<std::vector<uint8_t>>)> received;
  nod::signal<void()> health_check_response_received;
  nod::signal<void(const asio::error_code&)> error_occurred;
  nod::signal<void()> closed;

  peer(const peer&) = delete;

  peer(std::weak_ptr<dispatcher::dispatcher> weak_dispatcher,
       asio::local::stream_protocol::socket socket,
       const options& options,
       bool defer_ready = false)
      : dispatcher_client(weak_dispatcher),
        socket_(std::move(socket)),
        options_(options),
        defer_ready_(defer_ready),
        ready_(!defer_ready),
        ready_deadline_(socket_.get_executor()),
        heartbeat_timer_(socket_.get_executor()),
        heartbeat_deadline_(socket_.get_executor()),
        write_deadline_(socket_.get_executor()) {
  }

  ~peer() override {
    close_socket();
    detach_from_dispatcher();
  }

  void async_start() {
    asio::post(socket_.get_executor(), [self = shared_from_this()] {
      self->start_ready_deadline();
      self->start_heartbeat_timer();
      self->refresh_heartbeat_deadline();
      self->read_header();
    });
  }

  void async_close() {
    asio::post(socket_.get_executor(), [self = shared_from_this()] {
      self->close();
    });
  }

  void async_send(const std::vector<uint8_t>& data) {
    auto frame = protocol::make_user_data_frame(data);

    asio::post(socket_.get_executor(), [self = shared_from_this(), frame = std::move(frame)] {
      self->push_frame(frame);
    });
  }

  void async_send_health_check() {
    auto frame = protocol::make_health_check_frame();

    asio::post(socket_.get_executor(), [self = shared_from_this(), frame = std::move(frame)] {
      self->push_frame(frame);
    });
  }

private:
  void start_heartbeat_timer() {
    if (!options_.heartbeat_interval) {
      return;
    }

    heartbeat_timer_.expires_after(*options_.heartbeat_interval);

    auto self = shared_from_this();
    heartbeat_timer_.async_wait([self](const auto& error_code) {
      if (!error_code &&
          self->socket_.is_open()) {
        self->push_frame(protocol::make_heartbeat_frame());
        self->start_heartbeat_timer();
      }
    });
  }

  void refresh_heartbeat_deadline() {
    if (!options_.heartbeat_timeout) {
      return;
    }

    heartbeat_deadline_.expires_after(*options_.heartbeat_timeout);

    auto self = shared_from_this();
    heartbeat_deadline_.async_wait([self](const auto& error_code) {
      if (!error_code) {
        self->handle_error(asio::error::timed_out);
      }
    });
  }

  void start_ready_deadline() {
    if (!defer_ready_) {
      return;
    }

    ready_deadline_.expires_after(std::chrono::milliseconds(100));

    auto self = shared_from_this();
    ready_deadline_.async_wait([self](const auto& error_code) {
      if (!error_code) {
        self->ensure_ready();
      }
    });
  }

  void ensure_ready() {
    if (ready_) {
      return;
    }

    ready_ = true;
    ready_deadline_.cancel();

    enqueue_to_dispatcher([self = shared_from_this()] {
      self->ready();
    });
  }

  void read_header() {
    if (!socket_.is_open()) {
      return;
    }

    auto self = shared_from_this();
    asio::async_read(socket_,
                     asio::buffer(read_header_),
                     [self](auto&& error_code, auto bytes_transferred) {
                       if (error_code) {
                         self->handle_error(error_code);
                         return;
                       }

                       if (bytes_transferred != protocol::header_size) {
                         self->handle_error(asio::error::message_size);
                         return;
                       }

                       auto body_size = protocol::decode_uint32(self->read_header_);
                       if (body_size < protocol::type_size ||
                           body_size > self->options_.max_message_size + protocol::type_size) {
                         self->handle_error(asio::error::message_size);
                         return;
                       }

                       self->read_body_.resize(body_size);
                       self->read_body();
                     });
  }

  void read_body() {
    auto self = shared_from_this();
    asio::async_read(socket_,
                     asio::buffer(read_body_),
                     [self](auto&& error_code, auto bytes_transferred) {
                       if (error_code) {
                         self->handle_error(error_code);
                         return;
                       }

                       if (bytes_transferred != self->read_body_.size()) {
                         self->handle_error(asio::error::message_size);
                         return;
                       }

                       self->refresh_heartbeat_deadline();

                       auto type = static_cast<protocol::message_type>(self->read_body_[0]);
                       switch (type) {
                         case protocol::message_type::heartbeat:
                           self->ensure_ready();
                           break;

                         case protocol::message_type::user_data: {
                           self->ensure_ready();

                           auto v = std::make_shared<std::vector<uint8_t>>(std::begin(self->read_body_) + protocol::type_size,
                                                                           std::end(self->read_body_));
                           self->enqueue_to_dispatcher([self, v] {
                             self->received(v);
                           });
                           break;
                         }

                         case protocol::message_type::health_check:
                           if (self->ready_) {
                             self->handle_error(asio::error::operation_not_supported);
                           } else {
                             self->close_after_write_ = true;
                             self->push_frame(protocol::make_health_check_response_frame());
                           }
                           return;

                         case protocol::message_type::health_check_response:
                           self->enqueue_to_dispatcher([self] {
                             self->health_check_response_received();
                           });
                           break;
                       }

                       self->read_header();
                     });
  }

  void push_frame(std::vector<uint8_t> frame) {
    if (!socket_.is_open()) {
      return;
    }

    if (frame.size() > options_.max_message_size + protocol::header_size + protocol::type_size ||
        write_queue_.size() >= options_.max_send_queue_size) {
      handle_error(asio::error::no_buffer_space);
      return;
    }

    auto was_empty = write_queue_.empty();
    write_queue_.push_back(std::move(frame));

    if (was_empty) {
      write();
    }
  }

  void write() {
    if (!socket_.is_open() ||
        write_queue_.empty()) {
      return;
    }

    if (options_.write_timeout) {
      write_deadline_.expires_after(*options_.write_timeout);

      auto self = shared_from_this();
      write_deadline_.async_wait([self](const auto& error_code) {
        if (!error_code) {
          self->handle_error(asio::error::timed_out);
        }
      });
    }

    auto self = shared_from_this();
    asio::async_write(socket_,
                      asio::buffer(write_queue_.front()),
                      [self](auto&& error_code, auto) {
                        self->write_deadline_.cancel();

                        if (error_code) {
                          self->handle_error(error_code);
                          return;
                        }

                        self->write_queue_.pop_front();

                        if (self->write_queue_.empty() &&
                            self->close_after_write_) {
                          self->close();
                          return;
                        }

                        self->write();
                      });
  }

  void handle_error(const asio::error_code& error_code) {
    if (error_code == asio::error::operation_aborted) {
      return;
    }

    enqueue_to_dispatcher([self = shared_from_this(), error_code] {
      self->error_occurred(error_code);
    });

    close();
  }

  void close() {
    if (!socket_.is_open()) {
      return;
    }

    close_socket();

    enqueue_to_dispatcher([self = shared_from_this()] {
      self->closed();
    });
  }

  void close_socket() {
    asio::error_code error_code;
    ready_deadline_.cancel();
    heartbeat_timer_.cancel();
    heartbeat_deadline_.cancel();
    write_deadline_.cancel();
    socket_.cancel(error_code);
    socket_.close(error_code);
    write_queue_.clear();
  }

  asio::local::stream_protocol::socket socket_;
  options options_;
  bool defer_ready_;
  bool ready_;
  bool close_after_write_ = false;
  asio::steady_timer ready_deadline_;
  asio::steady_timer heartbeat_timer_;
  asio::steady_timer heartbeat_deadline_;
  asio::steady_timer write_deadline_;
  std::array<uint8_t, protocol::header_size> read_header_;
  std::vector<uint8_t> read_body_;
  std::deque<std::vector<uint8_t>> write_queue_;
};

} // namespace pqrs::unix_domain_stream::impl
