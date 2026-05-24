#include <atomic>
#include <array>
#include <boost/ut.hpp>
#include <iostream>
#include <pqrs/unix_domain_stream.hpp>
#include <thread>

namespace {

const std::filesystem::path server_socket_file_path("tmp/server.sock");

pqrs::unix_domain_stream::options make_options() {
  pqrs::unix_domain_stream::options options;
  options.max_message_size = 32 * 1024;
  options.max_send_queue_size = 128;
  options.reconnect_interval = std::chrono::milliseconds(100);
  options.write_timeout = std::chrono::milliseconds(1000);
  return options;
}

template <typename T>
bool wait_until(T predicate,
                std::chrono::milliseconds timeout = std::chrono::milliseconds(3000)) {
  auto deadline = std::chrono::steady_clock::now() + timeout;

  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  return predicate();
}

void prepare_socket_file_path(const std::filesystem::path& path) {
  std::error_code error_code;
  std::filesystem::create_directories(path.parent_path(), error_code);
  std::filesystem::remove(path, error_code);
}

} // namespace

int main() {
  using namespace boost::ut;
  using namespace boost::ut::literals;

  "unix_domain_stream::client_server"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::client_server)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    std::atomic<pqrs::unix_domain_stream::peer_id> connected_peer_id = 0;
    std::atomic<size_t> server_received_count = 0;
    std::atomic<size_t> client_received_count = 0;
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;

    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->bind_failed.connect([&](auto&& error_code) {
      std::cout << "bind_failed: " << error_code.message() << std::endl;
    });
    server->peer_connected.connect([&](auto peer_id, auto&&) {
      connected_peer_id = peer_id;
    });
    server->received.connect([&](auto peer_id, auto&& buffer) {
      server_received_count += buffer->size();
      server->async_send(peer_id, *buffer);
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    auto client = std::make_unique<pqrs::unix_domain_stream::client>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->received.connect([&](auto&& buffer) {
      client_received_count += buffer->size();
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));

    std::vector<uint8_t> data(32);
    data[0] = 10;
    data[1] = 20;
    data[2] = 30;
    client->async_send(data);

    expect(wait_until([&] { return client_received_count.load() == 32; }));

    expect(connected_peer_id.load() > 0);
    expect(server_received_count.load() == 32);
    expect(client_received_count.load() == 32);

    client = nullptr;
    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::client_reconnect"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::client_reconnect)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();

    std::atomic<size_t> connected_count = 0;

    auto client = std::make_unique<pqrs::unix_domain_stream::client>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    client->connected.connect([&](auto&&) {
      ++connected_count;
    });
    client->async_start();

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    expect(connected_count.load() == 0);

    std::atomic_bool server_bound = false;
    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    expect(wait_until([&] { return connected_count.load() == 1; }));
    expect(connected_count.load() == 1);

    client = nullptr;
    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::max_message_size"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::max_message_size)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    options.max_message_size = 8;
    std::atomic_bool server_bound = false;
    std::atomic_bool error_occurred = false;

    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    std::atomic_bool client_connected = false;
    auto client = std::make_unique<pqrs::unix_domain_stream::client>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->error_occurred.connect([&](auto&& error_code) {
      expect(error_code == asio::error::no_buffer_space);
      error_occurred = true;
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));

    client->async_send(std::vector<uint8_t>(9, 42));

    expect(wait_until([&] { return error_occurred.load(); }));

    client = nullptr;
    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_check"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_check)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    options.server_check_interval = std::chrono::milliseconds(100);
    options.server_check_timeout = std::chrono::milliseconds(1000);

    std::atomic<size_t> bound_count = 0;
    std::atomic<size_t> closed_count = 0;
    std::atomic<size_t> peer_connected_count = 0;
    std::atomic<size_t> client_received_count = 0;

    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      ++bound_count;
    });
    server->closed.connect([&] {
      ++closed_count;
    });
    server->peer_connected.connect([&](auto, auto&&) {
      ++peer_connected_count;
    });
    server->received.connect([&](auto peer_id, auto&& buffer) {
      server->async_send(peer_id, *buffer);
    });
    server->async_start();

    expect(wait_until([&] { return bound_count.load() == 1; }));

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    expect(peer_connected_count.load() == 0);

    std::error_code error_code;
    std::filesystem::remove(server_socket_file_path, error_code);

    expect(wait_until([&] { return closed_count.load() >= 1; }));
    expect(wait_until([&] { return bound_count.load() >= 2; }));

    std::atomic_bool client_connected = false;
    auto client = std::make_unique<pqrs::unix_domain_stream::client>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->received.connect([&](auto&& buffer) {
      client_received_count += buffer->size();
    });
    client->async_start();

    expect(wait_until([&] { return client_connected.load(); }));

    client->async_send(std::vector<uint8_t>(16, 42));

    expect(wait_until([&] { return client_received_count.load() == 16; }));
    expect(peer_connected_count.load() == 1);

    client = nullptr;
    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::read_timeout"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::read_timeout)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    options.read_timeout = std::chrono::milliseconds(200);

    std::atomic_bool server_bound = false;
    std::atomic_bool peer_error_occurred = false;
    std::atomic_bool peer_closed = false;

    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_error_occurred.connect([&](auto, auto&& error_code) {
      expect(error_code == asio::error::timed_out);
      peer_error_occurred = true;
    });
    server->peer_closed.connect([&](auto) {
      peer_closed = true;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    asio::io_context io_ctx;
    asio::local::stream_protocol::socket socket(io_ctx);
    socket.connect(asio::local::stream_protocol::endpoint(server_socket_file_path));

    std::array<uint8_t, 4> header{0, 0, 0, 8};
    asio::write(socket, asio::buffer(header));

    expect(wait_until([&] { return peer_error_occurred.load(); }));
    expect(wait_until([&] { return peer_closed.load(); }));

    asio::error_code error_code;
    socket.close(error_code);

    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::heartbeat"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::heartbeat)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    options.heartbeat_interval = std::chrono::milliseconds(50);
    options.heartbeat_timeout = std::chrono::milliseconds(300);

    std::atomic<size_t> server_received_count = 0;
    std::atomic<size_t> client_received_count = 0;
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;

    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->received.connect([&](auto peer_id, auto&& buffer) {
      server_received_count += buffer->size();
      server->async_send(peer_id, *buffer);
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    auto client = std::make_unique<pqrs::unix_domain_stream::client>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->received.connect([&](auto&& buffer) {
      client_received_count += buffer->size();
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    client->async_send(std::vector<uint8_t>(16, 42));

    expect(wait_until([&] { return client_received_count.load() == 16; }));
    expect(server_received_count.load() == 16);
    expect(client_received_count.load() == 16);

    client = nullptr;
    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  return 0;
}
