#pragma once

// (C) Copyright Takayama Fumihiko 2026.
// Distributed under the Boost Software License, Version 1.0.
// (See https://www.boost.org/LICENSE_1_0.txt)

// `pqrs::unix_domain_stream::server` can be used safely in a multi-threaded environment.

#include "impl/credentials.hpp"
#include "impl/peer.hpp"
#include "options.hpp"
#include "peer_credentials.hpp"
#include <filesystem>
#include <nod/nod.hpp>
#include <pqrs/dispatcher.hpp>
#include <unordered_map>

namespace pqrs::unix_domain_stream {

using peer_id = uint64_t;

inline bool default_verify_peer(const peer_credentials&) {
  return true;
}

class server final : public dispatcher::extra::dispatcher_client {
public:
  nod::signal<void()> bound;
  nod::signal<void(const asio::error_code&)> bind_failed;
  nod::signal<void()> closed;
  nod::signal<void(peer_id, const peer_credentials&)> peer_connected;
  nod::signal<void(peer_id)> peer_closed;
  nod::signal<void(peer_id, const asio::error_code&)> peer_error_occurred;
  nod::signal<void(peer_id, not_null_shared_ptr_t<std::vector<uint8_t>>)> received;

  server(const server&) = delete;

  server(std::weak_ptr<dispatcher::dispatcher> weak_dispatcher,
         const std::filesystem::path& socket_file_path,
         const options& options = {},
         std::function<bool(const peer_credentials&)> verify_peer = default_verify_peer)
      : dispatcher_client(weak_dispatcher),
        socket_file_path_(socket_file_path),
        options_(options),
        verify_peer_(verify_peer),
        reconnect_timer_(*this),
        work_guard_(asio::make_work_guard(io_ctx_)) {
    io_ctx_thread_ = std::thread([this] {
      io_ctx_.run();
    });
  }

  ~server() override {
    detach_from_dispatcher([this] {
      stop();
    });

    asio::post(io_ctx_, [this] {
      close_acceptor();
      peers_.clear();
      work_guard_.reset();
    });

    if (io_ctx_thread_.joinable()) {
      io_ctx_thread_.join();
    }
  }

  void async_start() {
    enqueue_to_dispatcher([this] {
      bind();
    });
  }

  void async_stop() {
    enqueue_to_dispatcher([this] {
      stop();
    });
  }

  void async_send(peer_id id,
                  const std::vector<uint8_t>& data) {
    asio::post(io_ctx_, [this, id, data] {
      if (auto it = peers_.find(id);
          it != std::end(peers_)) {
        it->second->async_send(data);
      }
    });
  }

  void async_close_peer(peer_id id) {
    asio::post(io_ctx_, [this, id] {
      if (auto it = peers_.find(id);
          it != std::end(peers_)) {
        it->second->async_close();
        peers_.erase(it);
      }
    });
  }

private:
  void stop() {
    options_.reconnect_interval = std::nullopt;
    reconnect_timer_.stop();

    asio::post(io_ctx_, [this] {
      close_acceptor();
      peers_.clear();
    });
  }

  void bind() {
    asio::post(io_ctx_, [this] {
      if (acceptor_) {
        return;
      }

      std::error_code remove_error_code;
      std::filesystem::remove(socket_file_path_, remove_error_code);

      acceptor_ = std::make_unique<asio::local::stream_protocol::acceptor>(io_ctx_);

      asio::error_code error_code;
      acceptor_->open(asio::local::stream_protocol::endpoint(socket_file_path_).protocol(), error_code);
      if (error_code) {
        handle_bind_failed(error_code);
        return;
      }

      acceptor_->bind(asio::local::stream_protocol::endpoint(socket_file_path_), error_code);
      if (error_code) {
        handle_bind_failed(error_code);
        return;
      }

      acceptor_->listen(asio::socket_base::max_listen_connections, error_code);
      if (error_code) {
        handle_bind_failed(error_code);
        return;
      }

      enqueue_to_dispatcher([this] {
        bound();
      });

      accept();
    });
  }

  void handle_bind_failed(const asio::error_code& error_code) {
    close_acceptor();

    enqueue_to_dispatcher([this, error_code] {
      bind_failed(error_code);
      start_reconnect_timer();
    });
  }

  void accept() {
    if (!acceptor_) {
      return;
    }

    acceptor_->async_accept(
        [this](auto&& error_code, auto socket) {
          if (error_code) {
            if (error_code != asio::error::operation_aborted) {
              close_acceptor();

              enqueue_to_dispatcher([this] {
                closed();
                start_reconnect_timer();
              });
            }
            return;
          }

          auto credentials = impl::make_peer_credentials(socket);
          if (!verify_peer_(credentials)) {
            asio::error_code close_error_code;
            socket.close(close_error_code);
            accept();
            return;
          }

          auto id = ++next_peer_id_;
          auto p = std::make_shared<impl::peer>(weak_dispatcher_,
                                                std::move(socket),
                                                options_);
          peers_[id] = p;

          p->received.connect([this, id](auto&& buffer) {
            enqueue_to_dispatcher([this, id, buffer] {
              received(id, buffer);
            });
          });

          p->error_occurred.connect([this, id](auto&& error_code) {
            enqueue_to_dispatcher([this, id, error_code] {
              peer_error_occurred(id, error_code);
            });
          });

          p->closed.connect([this, id] {
            asio::post(io_ctx_, [this, id] {
              peers_.erase(id);
            });

            enqueue_to_dispatcher([this, id] {
              peer_closed(id);
            });
          });

          p->async_start();

          enqueue_to_dispatcher([this, id, credentials] {
            peer_connected(id, credentials);
          });

          accept();
        });
  }

  void close_acceptor() {
    asio::error_code error_code;
    if (acceptor_) {
      acceptor_->cancel(error_code);
      acceptor_->close(error_code);
      acceptor_ = nullptr;
    }

    std::error_code remove_error_code;
    std::filesystem::remove(socket_file_path_, remove_error_code);
  }

  void start_reconnect_timer() {
    if (options_.reconnect_interval) {
      reconnect_timer_.start(
          [this] {
            bind();
          },
          *options_.reconnect_interval);
    }
  }

  std::filesystem::path socket_file_path_;
  options options_;
  std::function<bool(const peer_credentials&)> verify_peer_;
  dispatcher::extra::timer reconnect_timer_;

  asio::io_context io_ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> work_guard_;
  std::thread io_ctx_thread_;
  std::unique_ptr<asio::local::stream_protocol::acceptor> acceptor_;
  std::unordered_map<peer_id, std::shared_ptr<impl::peer>> peers_;
  peer_id next_peer_id_ = 0;
};

} // namespace pqrs::unix_domain_stream
