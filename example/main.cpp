#include <atomic>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <pqrs/unix_domain_stream.hpp>

namespace {
auto global_wait = pqrs::make_thread_wait();

void output_received_data(pqrs::not_null_shared_ptr_t<std::vector<uint8_t>> buffer) {
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

void output_peer_credentials(const pqrs::unix_domain_stream::peer_credentials& credentials) {
  std::cout << "peer_pid: " << credentials.pid.value_or(-1) << std::endl;
  std::cout << "peer_uid: " << credentials.uid.value_or(-1) << std::endl;
  std::cout << "peer_gid: " << credentials.gid.value_or(-1) << std::endl;
}

std::vector<uint8_t> make_buffer(const std::string& string) {
  return std::vector<uint8_t>(std::begin(string), std::end(string));
}

void output_async_request_result(const std::string& label,
                                 const asio::error_code& error_code,
                                 std::shared_ptr<std::vector<uint8_t>> response) {
  if (error_code) {
    std::cout << label << " failed: " << error_code.message() << std::endl;
    return;
  }

  std::cout << label << " response size:" << response->size() << std::endl;
  output_received_data(response);
}
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

  pqrs::unix_domain_stream::options options(
      pqrs::unix_domain_stream::options::initialization_parameters{
          .max_send_queue_size = 128,
      });

  // server

  auto server = std::make_shared<pqrs::unix_domain_stream::server>(dispatcher,
                                                                   server_socket_file_path,
                                                                   options);

  // client

  auto client = std::make_shared<pqrs::unix_domain_stream::client>(dispatcher,
                                                                   server_socket_file_path,
                                                                   options);

  std::atomic_bool initial_messages_sent = false;

  server->bound.connect([&client] {
    std::cout << "server bound" << std::endl;
    client->async_start();
  });
  server->bind_failed.connect([](auto&& error_code) {
    std::cout << "server bind_failed:" << error_code.message() << std::endl;
  });
  server->closed.connect([] {
    std::cout << "server closed" << std::endl;
  });
  server->peer_connected.connect([](auto peer_id, auto&& credentials) {
    std::cout << "server peer_connected peer_id:" << peer_id << std::endl;
    output_peer_credentials(credentials);
  });
  server->peer_closed.connect([](auto peer_id) {
    std::cout << "server peer_closed peer_id:" << peer_id << std::endl;
  });
  server->peer_error_occurred.connect([](auto peer_id, auto&& error_code) {
    std::cout << "server peer_error_occurred peer_id:" << peer_id << " " << error_code.message() << std::endl;
  });
  server->received.connect([&server](auto peer_id, auto&& buffer) {
    std::cout << "server received peer_id:" << peer_id << " size:" << buffer->size() << std::endl;
    output_received_data(buffer);

    server->async_send(peer_id, *buffer);
  });
  server->request_received.connect([&server](auto peer_id, auto request_id, auto&& buffer) {
    std::cout << "server request_received peer_id:" << peer_id << " request_id:" << request_id << " size:" << buffer->size() << std::endl;
    output_received_data(buffer);

    auto response = make_buffer("response for ");
    response.insert(std::end(response), std::begin(*buffer), std::end(*buffer));
    server->async_respond(peer_id, request_id, response);
  });

  client->connected.connect([&client, &initial_messages_sent](auto&& credentials) {
    std::cout << "client connected" << std::endl;
    output_peer_credentials(credentials);

    if (initial_messages_sent.exchange(true)) {
      return;
    }

    {
      std::vector<uint8_t> buffer;
      buffer.push_back('1');
      client->async_send(buffer);
    }
    {
      std::vector<uint8_t> buffer;
      buffer.push_back('1');
      buffer.push_back('2');
      client->async_send(buffer);
    }
    {
      std::vector<uint8_t> buffer(30 * 1024, '3');
      client->async_send(buffer);
    }
    {
      client->async_send(make_buffer("Type control-c to quit."));
    }
    {
      client->async_request(make_buffer("request 1"),
                            [](const auto& error_code, auto response) {
                              output_async_request_result("client async_request 1", error_code, response);
                            });
      client->async_request(make_buffer("request 2"),
                            [](const auto& error_code, auto response) {
                              output_async_request_result("client async_request 2", error_code, response);
                            });
    }
  });
  client->connect_failed.connect([](auto&& error_code) {
    std::cout << "client connect_failed:" << error_code.message() << std::endl;
  });
  client->closed.connect([] {
    std::cout << "client closed" << std::endl;
  });
  client->error_occurred.connect([](auto&& error_code) {
    std::cout << "client error_occurred:" << error_code.message() << std::endl;
  });
  client->received.connect([](auto&& buffer) {
    std::cout << "client received size:" << buffer->size() << std::endl;
    output_received_data(buffer);
  });

  server->async_start();

  // ============================================================

  global_wait->wait_notice();

  // ============================================================

  client = nullptr;
  server = nullptr;

  dispatcher->terminate();
  dispatcher = nullptr;

  std::cout << "finished" << std::endl;

  return 0;
}
