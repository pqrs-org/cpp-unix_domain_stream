#include <atomic>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <pqrs/unix_domain_stream.hpp>

namespace {
auto global_wait = pqrs::make_thread_wait();

class example_app final {
public:
  example_app(pqrs::not_null_shared_ptr_t<pqrs::dispatcher::dispatcher> dispatcher,
              const std::filesystem::path& server_socket_file_path,
              const pqrs::unix_domain_stream::server_options& server_options,
              const pqrs::unix_domain_stream::client_options& client_options)
      : server_(std::make_unique<pqrs::unix_domain_stream::server>(
            dispatcher.get(),
            server_socket_file_path,
            server_options,
            [](const auto& credentials) {
              std::cout << "server verify_peer" << std::endl;
              example_app::output_peer_credentials(credentials);
              return true;
            })),
        client_(std::make_unique<pqrs::unix_domain_stream::client>(dispatcher.get(),
                                                                   server_socket_file_path,
                                                                   client_options)) {
    server_->bound.connect([this] {
      std::cout << "server bound" << std::endl;
      client_->async_start();
    });
    server_->bind_failed.connect([](auto&& error_code) {
      std::cout << "server bind_failed:" << error_code.message() << std::endl;
    });
    server_->closed.connect([] {
      std::cout << "server closed" << std::endl;
    });
    server_->peer_connected.connect([](auto peer_id, auto&& credentials) {
      std::cout << "server peer_connected peer_id:" << peer_id << std::endl;
      example_app::output_peer_credentials(credentials);
    });
    server_->peer_closed.connect([](auto peer_id) {
      std::cout << "server peer_closed peer_id:" << peer_id << std::endl;
    });
    server_->peer_error_occurred.connect([](auto peer_id, auto&& error_code) {
      std::cout << "server peer_error_occurred peer_id:" << peer_id << " " << error_code.message() << std::endl;
    });
    server_->received.connect([this](auto peer_id, auto&& buffer) {
      std::cout << "server received peer_id:" << peer_id << " size:" << buffer->size() << std::endl;
      output_received_data(buffer);

      server_->async_send(peer_id, *buffer);
    });
    server_->request_received.connect([this](auto peer_id, auto request_id, auto&& buffer) {
      std::cout << "server request_received peer_id:" << peer_id << " request_id:" << request_id << " size:" << buffer->size() << std::endl;
      output_received_data(buffer);

      auto response = make_buffer("response for ");
      response.insert(std::end(response), std::begin(*buffer), std::end(*buffer));
      server_->async_respond(peer_id, request_id, response);
    });

    client_->connected.connect([this](auto&& credentials) {
      std::cout << "client connected" << std::endl;
      output_peer_credentials(credentials);

      if (initial_messages_sent_.exchange(true)) {
        return;
      }

      {
        std::vector<uint8_t> buffer;
        buffer.push_back('1');
        client_->async_send(buffer);
      }
      {
        std::vector<uint8_t> buffer;
        buffer.push_back('1');
        buffer.push_back('2');
        client_->async_send(buffer);
      }
      {
        std::vector<uint8_t> buffer(30 * 1024, '3');
        client_->async_send(buffer);
      }
      {
        client_->async_send(make_buffer("Type control-c to quit."));
      }
      {
        client_->async_request(make_buffer("request 1"),
                               [](const auto& error_code, auto response) {
                                 example_app::output_async_request_result("client async_request 1", error_code, response);
                               });
        client_->async_request(make_buffer("request 2"),
                               [](const auto& error_code, auto response) {
                                 example_app::output_async_request_result("client async_request 2", error_code, response);
                               });
      }
    });
    client_->connect_failed.connect([](auto&& error_code) {
      std::cout << "client connect_failed:" << error_code.message() << std::endl;
    });
    client_->peer_verification_failed.connect([](auto&& credentials) {
      std::cout << "client peer_verification_failed" << std::endl;
      example_app::output_peer_credentials(credentials);
    });
    client_->closed.connect([] {
      std::cout << "client closed" << std::endl;
    });
    client_->error_occurred.connect([](auto&& error_code) {
      std::cout << "client error_occurred:" << error_code.message() << std::endl;
    });
    client_->received.connect([](auto&& buffer) {
      std::cout << "client received size:" << buffer->size() << std::endl;
      example_app::output_received_data(buffer);
    });
    client_->request_received.connect([this](auto request_id, auto&& buffer) {
      std::cout << "client request_received request_id:" << request_id << " size:" << buffer->size() << std::endl;
      output_received_data(buffer);

      auto response = make_buffer("response for ");
      response.insert(std::end(response), std::begin(*buffer), std::end(*buffer));
      client_->async_respond(request_id, response);
    });
  }

  ~example_app() {
    server_ = nullptr;
    client_ = nullptr;
  }

  void async_start() {
    server_->async_start();
  }

private:
  static void output_received_data(pqrs::not_null_shared_ptr_t<std::vector<uint8_t>> buffer) {
    if (!buffer->empty()) {
      std::cout << "buffer: `";
      int count = 0;
      for (const auto& c : *buffer) {
        std::cout << c;
        ++count;
        if (count > 40) {
          std::cout << "... (" << buffer->size() << "bytes)";
          break;
        }
      }
      std::cout << "`";
      std::cout << std::endl;
    }
  }

  static void output_peer_credentials(const pqrs::unix_domain_stream::peer_credentials& credentials) {
    std::cout << "peer_pid: " << credentials.pid.value_or(-1) << std::endl;
    std::cout << "peer_uid: " << credentials.uid.value_or(-1) << std::endl;
    std::cout << "peer_gid: " << credentials.gid.value_or(-1) << std::endl;
  }

  static std::vector<uint8_t> make_buffer(const std::string& string) {
    return std::vector<uint8_t>(std::begin(string), std::end(string));
  }

  static void output_async_request_result(const std::string& label,
                                          const asio::error_code& error_code,
                                          std::shared_ptr<std::vector<uint8_t>> response) {
    if (error_code) {
      std::cout << label << " failed: " << error_code.message() << std::endl;
      return;
    }

    std::cout << label << " response size:" << response->size() << std::endl;
    output_received_data(response);
  }

  std::unique_ptr<pqrs::unix_domain_stream::server> server_;
  std::unique_ptr<pqrs::unix_domain_stream::client> client_;
  std::atomic_bool initial_messages_sent_ = false;
};
} // namespace

int main() {
  std::signal(SIGINT, [](int) {
    global_wait->notify();
  });

  auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
  auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

  std::filesystem::path server_socket_file_path("tmp/server.sock");
  std::error_code error_code;
  std::filesystem::create_directories(server_socket_file_path.parent_path(), error_code);

  pqrs::unix_domain_stream::server_options server_options{
      {
          .max_send_queue_size = 128,
      },
      {},
  };

  pqrs::unix_domain_stream::client_options client_options{
      {
          .max_send_queue_size = 128,
      },
      {},
  };

  auto app = std::make_unique<example_app>(dispatcher,
                                           server_socket_file_path,
                                           server_options,
                                           client_options);
  app->async_start();

  // ============================================================

  global_wait->wait_notice();

  // ============================================================

  app = nullptr;

  dispatcher->terminate();
  dispatcher = nullptr;

  std::cout << "finished" << std::endl;

  return 0;
}
