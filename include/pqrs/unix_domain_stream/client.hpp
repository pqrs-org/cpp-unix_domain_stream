#pragma once

// (C) Copyright Takayama Fumihiko 2026.
// Distributed under the Boost Software License, Version 1.0.
// (See https://www.boost.org/LICENSE_1_0.txt)

// `pqrs::unix_domain_stream::client` can be used safely in a multi-threaded environment.

#include "impl/credentials.hpp"
#include "impl/peer.hpp"
#include "options.hpp"
#include "peer_credentials.hpp"
#include <atomic>
#include <filesystem>
#include <nod/nod.hpp>
#include <pqrs/dispatcher.hpp>

namespace pqrs::unix_domain_stream {

class client final : public dispatcher::extra::dispatcher_client {
public:
  nod::signal<void(const peer_credentials&)> connected;
  nod::signal<void(const asio::error_code&)> connect_failed;
  nod::signal<void()> closed;
  nod::signal<void(const asio::error_code&)> error_occurred;
  nod::signal<void(not_null_shared_ptr_t<std::vector<uint8_t>>)> received;

  client(const client&) = delete;

  client(std::weak_ptr<dispatcher::dispatcher> weak_dispatcher,
         const std::filesystem::path& socket_file_path,
         const options& options = {})
      : dispatcher_client(weak_dispatcher),
        socket_file_path_(socket_file_path),
        options_(options),
        reconnect_timer_(*this),
        work_guard_(asio::make_work_guard(io_ctx_)) {
    io_ctx_thread_ = std::thread([this] {
      io_ctx_.run();
    });
  }

  ~client() override {
    detach_from_dispatcher([this] {
      stop();
    });

    asio::post(io_ctx_, [this] {
      if (peer_) {
        peer_->async_close();
        peer_ = nullptr;
      }
      work_guard_.reset();
    });

    if (io_ctx_thread_.joinable()) {
      io_ctx_thread_.join();
    }
  }

  void async_start() {
    enqueue_to_dispatcher([this] {
      stopped_ = false;
      connect();
    });
  }

  void async_stop() {
    enqueue_to_dispatcher([this] {
      stop();
    });
  }

  void async_send(const std::vector<uint8_t>& data) {
    asio::post(io_ctx_, [this, data] {
      if (peer_) {
        peer_->async_send(data);
      }
    });
  }

private:
  void stop() {
    stopped_ = true;
    reconnect_timer_.stop();

    asio::post(io_ctx_, [this] {
      if (peer_) {
        peer_->async_close();
        peer_ = nullptr;
      }
    });
  }

  void connect() {
    asio::post(io_ctx_, [this] {
      if (stopped_ ||
          peer_) {
        return;
      }

      auto socket = std::make_shared<asio::local::stream_protocol::socket>(io_ctx_);

      socket->async_connect(
          asio::local::stream_protocol::endpoint(socket_file_path_),
          [this, socket](auto&& error_code) mutable {
            if (stopped_) {
              asio::error_code close_error_code;
              socket->close(close_error_code);
              return;
            }

            if (error_code) {
              enqueue_to_dispatcher([this, error_code] {
                connect_failed(error_code);
                start_reconnect_timer();
              });
              return;
            }

            auto credentials = impl::make_peer_credentials(*socket);

            peer_ = std::make_shared<impl::peer>(weak_dispatcher_,
                                                 std::move(*socket),
                                                 options_);

            peer_->received.connect([this](auto&& buffer) {
              enqueue_to_dispatcher([this, buffer] {
                received(buffer);
              });
            });

            peer_->error_occurred.connect([this](auto&& error_code) {
              enqueue_to_dispatcher([this, error_code] {
                error_occurred(error_code);
              });
            });

            peer_->closed.connect([this] {
              asio::post(io_ctx_, [this] {
                peer_ = nullptr;
              });

              enqueue_to_dispatcher([this] {
                closed();
                start_reconnect_timer();
              });
            });

            peer_->async_start();

            enqueue_to_dispatcher([this, credentials] {
              connected(credentials);
            });
          });
    });
  }

  void start_reconnect_timer() {
    if (stopped_) {
      return;
    }

    reconnect_timer_.start(
        [this] {
          connect();
        },
        options_.reconnect_interval);
  }

  std::filesystem::path socket_file_path_;
  options options_;
  dispatcher::extra::timer reconnect_timer_;
  std::atomic_bool stopped_ = true;

  asio::io_context io_ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> work_guard_;
  std::thread io_ctx_thread_;
  std::shared_ptr<impl::peer> peer_;
};

} // namespace pqrs::unix_domain_stream
