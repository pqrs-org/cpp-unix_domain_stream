#include <array>
#include <asio/local/connect_pair.hpp>
#include <atomic>
#include <boost/ut.hpp>
#include <future>
#include <iostream>
#include <pqrs/unix_domain_stream.hpp>
#include <pqrs/unix_domain_stream/impl/protocol.hpp>
#include <thread>

namespace {

const std::filesystem::path server_socket_file_path("tmp/server.sock");

using async_request_test_result = std::pair<asio::error_code, std::shared_ptr<std::vector<uint8_t>>>;

pqrs::unix_domain_stream::options make_options() {
  return pqrs::unix_domain_stream::options(
      pqrs::unix_domain_stream::options::initialization_parameters{
          .max_send_queue_size = 128,
          .reconnect_interval = std::chrono::milliseconds(100),
          .write_timeout = std::chrono::milliseconds(1000),
      });
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

bool wait_dispatcher_barrier(const std::shared_ptr<pqrs::dispatcher::dispatcher>& dispatcher,
                             std::chrono::milliseconds timeout = std::chrono::milliseconds(3000)) {
  pqrs::dispatcher::extra::dispatcher_client barrier(dispatcher);
  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future();

  if (!barrier.enqueue_to_dispatcher([promise] {
        promise->set_value();
      })) {
    barrier.detach_from_dispatcher();
    return false;
  }

  auto result = future.wait_for(timeout) == std::future_status::ready;
  barrier.detach_from_dispatcher();

  return result;
}

} // namespace

int main() {
  using namespace boost::ut;
  using namespace boost::ut::literals;

  "unix_domain_stream::options_initialization_parameters"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::options_initialization_parameters)" << std::endl;

    // Ensure every initialization parameter is copied into options as-is.
    pqrs::unix_domain_stream::options options(
        pqrs::unix_domain_stream::options::initialization_parameters{
            .max_message_size = 123,
            .max_send_queue_size = 456,
            .reconnect_interval = std::chrono::milliseconds(789),
            .server_check_interval = std::chrono::milliseconds(234),
            .server_check_timeout = std::chrono::milliseconds(345),
            .heartbeat_interval = std::chrono::milliseconds(456),
            .heartbeat_timeout = std::chrono::milliseconds(567),
            .read_timeout = std::chrono::milliseconds(678),
            .write_timeout = std::chrono::milliseconds(890),
        });

    expect(options.max_message_size == 123_i);
    expect(options.max_send_queue_size == 456_i);
    expect(options.reconnect_interval == std::chrono::milliseconds(789));
    expect(options.server_check_interval == std::chrono::milliseconds(234));
    expect(options.server_check_timeout == std::chrono::milliseconds(345));
    expect(options.heartbeat_interval == std::chrono::milliseconds(456));
    expect(options.heartbeat_timeout == std::chrono::milliseconds(567));
    expect(options.read_timeout == std::chrono::milliseconds(678));
    expect(options.write_timeout == std::chrono::milliseconds(890));
  };

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

    // Echo any server-side user data back to the connected client.
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

    // Verify the normal client -> server -> client data path.
    std::vector<uint8_t> data(32);
    data[0] = 10;
    data[1] = 20;
    data[2] = 30;
    client->async_send(data);

    expect(wait_until([&] { return client_received_count.load() == 32_i; }));

    expect(connected_peer_id.load() > 0_i);
    expect(server_received_count.load() == 32_i);
    expect(client_received_count.load() == 32_i);

    client = nullptr;
    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::many_clients"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::many_clients)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    // Stress a single server with many regular client instances in one process.
    // This exercises peer lifetime, dispatcher signal delivery, and socket close
    // paths more directly than running the same test binary in parallel.
    constexpr size_t client_count = 256;
    constexpr size_t payload_size = 16;

    auto options = pqrs::unix_domain_stream::options(
        pqrs::unix_domain_stream::options::initialization_parameters{
            .max_send_queue_size = 128,
            .reconnect_interval = std::chrono::milliseconds(100),
            .server_check_interval = std::chrono::milliseconds(60000),
            .read_timeout = std::chrono::milliseconds(3000),
            .write_timeout = std::chrono::milliseconds(3000),
        });

    std::atomic_bool server_bound = false;
    std::atomic_size_t server_peer_connected_count = 0;
    std::atomic_size_t client_connected_count = 0;
    std::atomic_size_t server_received_count = 0;
    std::atomic_size_t client_received_count = 0;

    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto, auto&&) {
      ++server_peer_connected_count;
    });
    server->received.connect([&](auto peer_id, auto&& buffer) {
      server_received_count += buffer->size();
      server->async_send(peer_id, *buffer);
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    std::vector<std::unique_ptr<pqrs::unix_domain_stream::client>> clients;
    clients.reserve(client_count);

    for (size_t i = 0; i < client_count; ++i) {
      auto client = std::make_unique<pqrs::unix_domain_stream::client>(dispatcher,
                                                                       server_socket_file_path,
                                                                       options);
      client->connected.connect([&](auto&&) {
        ++client_connected_count;
      });
      client->received.connect([&](auto&& buffer) {
        client_received_count += buffer->size();
      });
      client->async_start();

      clients.push_back(std::move(client));
    }

    expect(wait_until([&] { return client_connected_count.load() == client_count; },
                      std::chrono::milliseconds(30000)));
    expect(wait_until([&] { return server_peer_connected_count.load() == client_count; },
                      std::chrono::milliseconds(30000)));

    for (auto& client : clients) {
      client->async_send(std::vector<uint8_t>(payload_size, 42));
    }

    expect(wait_until([&] { return server_received_count.load() == client_count * payload_size; },
                      std::chrono::milliseconds(30000)));
    expect(wait_until([&] { return client_received_count.load() == client_count * payload_size; },
                      std::chrono::milliseconds(30000)));

    // Drop some clients before the rest to make peer close and owner destruction
    // interleave with the server-side peer cleanup path.
    for (size_t i = 0; i < clients.size(); i += 2) {
      clients[i] = nullptr;
    }

    clients.clear();
    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::connected_client_survives_socket_file_removal"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::connected_client_survives_socket_file_removal)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    // Keep the server health check out of this test. This case is only about
    // the already accepted socket, not the server's rebind recovery path.
    auto options = pqrs::unix_domain_stream::options(
        pqrs::unix_domain_stream::options::initialization_parameters{
            .max_send_queue_size = 128,
            .reconnect_interval = std::chrono::milliseconds(100),
            .server_check_interval = std::chrono::milliseconds(60000),
            .write_timeout = std::chrono::milliseconds(1000),
        });
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
    expect(wait_until([&] { return connected_peer_id.load() > 0_i; }));

    // Unlinking the socket pathname should not affect the connected socket
    // descriptors held by the client and server.
    std::error_code error_code;
    std::filesystem::remove(server_socket_file_path, error_code);
    expect(!std::filesystem::exists(server_socket_file_path));

    // Prove the existing connection still works by sending user data through
    // the established client -> server -> client echo path.
    client->async_send(std::vector<uint8_t>(24, 42));

    expect(wait_until([&] { return client_received_count.load() == 24_i; }));
    expect(server_received_count.load() == 24_i);

    client = nullptr;
    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::verify_peer_runs_on_dispatcher"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::verify_peer_runs_on_dispatcher)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    pqrs::unix_domain_stream::options options(
        pqrs::unix_domain_stream::options::initialization_parameters{
            .server_check_interval = std::chrono::milliseconds(60000),
        });

    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;
    std::atomic_bool verify_peer_called = false;
    std::promise<std::thread::id> verify_peer_thread_id_promise;
    std::promise<std::thread::id> peer_connected_thread_id_promise;

    // Capture the dispatcher thread used by verify_peer and peer_connected.
    auto server = std::make_unique<pqrs::unix_domain_stream::server>(
        dispatcher,
        server_socket_file_path,
        options,
        [&](auto&&) {
          if (!verify_peer_called.exchange(true)) {
            verify_peer_thread_id_promise.set_value(std::this_thread::get_id());
          }
          return true;
        });
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto, auto&&) {
      peer_connected_thread_id_promise.set_value(std::this_thread::get_id());
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    auto client = std::make_unique<pqrs::unix_domain_stream::client>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));

    auto verify_peer_thread_id_future = verify_peer_thread_id_promise.get_future();
    auto peer_connected_thread_id_future = peer_connected_thread_id_promise.get_future();

    expect(verify_peer_thread_id_future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);
    expect(peer_connected_thread_id_future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);
    // verify_peer must run on the dispatcher so callers can use dispatcher-bound state safely.
    expect(verify_peer_thread_id_future.get() == peer_connected_thread_id_future.get());

    client = nullptr;
    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_drops_unverified_peer_data"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_drops_unverified_peer_data)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = pqrs::unix_domain_stream::options(
        pqrs::unix_domain_stream::options::initialization_parameters{
            .max_send_queue_size = 128,
            .reconnect_interval = std::chrono::milliseconds(100),
            .server_check_interval = std::chrono::milliseconds(60000),
            .read_timeout = std::chrono::milliseconds(1000),
            .write_timeout = std::chrono::milliseconds(1000),
        });

    std::atomic_bool server_bound = false;
    std::atomic_size_t verify_peer_count = 0;
    std::atomic_size_t peer_connected_count = 0;
    std::atomic_size_t server_received_count = 0;
    std::atomic_size_t server_request_received_count = 0;

    // Reject the first peer and accept the second peer, without depending on
    // platform-specific peer_credentials values.
    auto server = std::make_unique<pqrs::unix_domain_stream::server>(
        dispatcher,
        server_socket_file_path,
        options,
        [&](auto&&) {
          auto count = ++verify_peer_count;
          return count % 2 == 0;
        });
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto, auto&&) {
      ++peer_connected_count;
    });
    server->received.connect([&](auto, auto&& buffer) {
      server_received_count += buffer->size();
    });
    server->request_received.connect([&](auto, auto, auto&&) {
      ++server_request_received_count;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    // Use raw protocol frames so the first peer can send data immediately after
    // connecting, before the server closes the unverified connection.
    auto user_data_frame = pqrs::unix_domain_stream::impl::protocol::make_user_data_frame(std::vector<uint8_t>{1, 2, 3});
    auto request_frame = pqrs::unix_domain_stream::impl::protocol::make_request_frame(1, std::vector<uint8_t>{4, 5, 6});

    asio::io_context io_ctx;

    // The rejected peer sends valid user data and request frames, but they must
    // not be exposed through the server's public signals.
    asio::local::stream_protocol::socket rejected_socket(io_ctx);
    rejected_socket.connect(asio::local::stream_protocol::endpoint(server_socket_file_path));
    asio::write(rejected_socket, asio::buffer(user_data_frame));
    asio::write(rejected_socket, asio::buffer(request_frame));

    expect(wait_until([&] { return verify_peer_count.load() == 1_i; }));
    expect(wait_dispatcher_barrier(dispatcher));

    expect(peer_connected_count.load() == 0_i);
    expect(server_received_count.load() == 0_i);
    expect(server_request_received_count.load() == 0_i);

    asio::error_code close_error_code;
    rejected_socket.close(close_error_code);

    // A verified peer should still be usable and should receive normal signal
    // delivery for the same frame types.
    asio::local::stream_protocol::socket accepted_socket(io_ctx);
    accepted_socket.connect(asio::local::stream_protocol::endpoint(server_socket_file_path));

    expect(wait_until([&] { return peer_connected_count.load() == 1_i; }));
    expect(verify_peer_count.load() == 2_i);

    asio::write(accepted_socket, asio::buffer(user_data_frame));
    asio::write(accepted_socket, asio::buffer(request_frame));

    expect(wait_until([&] { return server_received_count.load() == 3_i; }));
    expect(wait_until([&] { return server_request_received_count.load() == 1_i; }));

    accepted_socket.close(close_error_code);

    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::client_peer_verification_failed"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::client_peer_verification_failed)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;
    std::atomic_size_t peer_verification_failed_count = 0;

    // The server accepts connections normally; the client rejects the server peer.
    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    auto client = std::make_unique<pqrs::unix_domain_stream::client>(
        dispatcher,
        server_socket_file_path,
        options,
        [](auto&&) {
          return false;
        });
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->peer_verification_failed.connect([&](auto&&) {
      ++peer_verification_failed_count;
    });
    client->async_start();

    // A rejected server peer should notify failure and never publish connected.
    expect(wait_until([&] { return peer_verification_failed_count.load() >= 1_i; }));
    expect(!client_connected.load());

    client = nullptr;
    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::peer_dispatcher_callbacks_do_not_keep_peer_alive"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::peer_dispatcher_callbacks_do_not_keep_peer_alive)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    asio::io_context io_ctx;
    auto work_guard = asio::make_work_guard(io_ctx);
    std::thread io_ctx_thread([&] {
      io_ctx.run();
    });

    asio::local::stream_protocol::socket peer_socket(io_ctx);
    asio::local::stream_protocol::socket raw_socket(io_ctx);
    asio::local::connect_pair(peer_socket, raw_socket);

    pqrs::dispatcher::extra::dispatcher_client blocker(dispatcher);
    std::promise<void> blocker_started_promise;
    auto blocker_started_future = blocker_started_promise.get_future();
    std::promise<void> unblock_promise;
    auto unblock_future = unblock_promise.get_future().share();

    blocker.enqueue_to_dispatcher([&] {
      blocker_started_promise.set_value();
      unblock_future.wait();
    });
    expect(blocker_started_future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto options = make_options();
    auto peer = std::make_shared<pqrs::unix_domain_stream::impl::peer>(dispatcher,
                                                                       std::move(peer_socket),
                                                                       options);
    std::weak_ptr<pqrs::unix_domain_stream::impl::peer> weak_peer(peer);

    peer->async_start();

    // Wait on the peer io_context until after ready_deadline should have
    // enqueued peer->ready behind the blocked dispatcher job.
    auto ready_deadline_passed = std::make_shared<asio::steady_timer>(io_ctx);
    std::promise<void> ready_deadline_passed_promise;
    auto ready_deadline_passed_future = ready_deadline_passed_promise.get_future();
    ready_deadline_passed->expires_after(std::chrono::milliseconds(300));
    ready_deadline_passed->async_wait([ready_deadline_passed, &ready_deadline_passed_promise](auto&&) {
      ready_deadline_passed_promise.set_value();
    });
    expect(ready_deadline_passed_future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    // The queued dispatcher callback must not keep peer alive after the owner
    // closes it.
    peer->async_close();
    peer.reset();

    expect(wait_until([&] { return weak_peer.expired(); }));

    unblock_promise.set_value();
    blocker.detach_from_dispatcher();

    asio::error_code error_code;
    raw_socket.close(error_code);

    work_guard.reset();
    if (io_ctx_thread.joinable()) {
      io_ctx_thread.join();
    }

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::client_async_request"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::client_async_request)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;

    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      server_bound = true;
    });

    pqrs::unix_domain_stream::peer_id first_peer_id = 0;
    pqrs::unix_domain_stream::request_id first_request_id = 0;
    // Delay the response to request 1 until request 2 arrives, to confirm
    // responses are matched by request_id rather than completion order.
    server->request_received.connect([&](auto peer_id, auto request_id, auto&& buffer) {
      if (buffer->at(0) == 1) {
        first_peer_id = peer_id;
        first_request_id = request_id;
        return;
      }

      server->async_respond(peer_id, request_id, std::vector<uint8_t>{20});
      server->async_respond(first_peer_id, first_request_id, std::vector<uint8_t>{10});
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    auto client = std::make_unique<pqrs::unix_domain_stream::client>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));

    std::promise<async_request_test_result> promise1;
    std::promise<async_request_test_result> promise2;
    auto future1 = promise1.get_future();
    auto future2 = promise2.get_future();

    // Send two concurrent requests and complete them in reverse order.
    client->async_request(std::vector<uint8_t>{1},
                          [&promise1](const auto& error_code, auto response) {
                            promise1.set_value({error_code, response});
                          });
    client->async_request(std::vector<uint8_t>{2},
                          [&promise2](const auto& error_code, auto response) {
                            promise2.set_value({error_code, response});
                          });

    expect(future1.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);
    expect(future2.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto [error_code1, response1] = future1.get();
    auto [error_code2, response2] = future2.get();

    // Each callback should receive the response for its own request_id.
    expect(!error_code1);
    expect(!error_code2);
    expect(response1 != nullptr);
    expect(response2 != nullptr);
    expect(*response1 == std::vector<uint8_t>{10});
    expect(*response2 == std::vector<uint8_t>{20});

    client = nullptr;
    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::client_async_request_timeout"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::client_async_request_timeout)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;
    std::atomic_size_t server_request_received_count = 0;

    // Accept the request on the server, but intentionally do not respond.
    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->request_received.connect([&](auto, auto, auto&&) {
      ++server_request_received_count;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    auto client = std::make_unique<pqrs::unix_domain_stream::client>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));

    std::promise<async_request_test_result> promise;
    auto future = promise.get_future();

    // The client-side per-request timeout should complete the callback.
    client->async_request(std::vector<uint8_t>{1},
                          std::chrono::milliseconds(100),
                          [&promise](const auto& error_code, auto response) {
                            promise.set_value({error_code, response});
                          });

    expect(future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto [error_code, response] = future.get();

    // Timeout is local to the pending request; the server still saw the request.
    expect(error_code == asio::error::timed_out);
    expect(response == nullptr);
    expect(server_request_received_count.load() == 1_i);

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
    std::atomic<size_t> connect_failed_count = 0;

    // Start the client before the server so the first connect attempt fails.
    auto client = std::make_unique<pqrs::unix_domain_stream::client>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    client->connected.connect([&](auto&&) {
      ++connected_count;
    });
    client->connect_failed.connect([&](auto&&) {
      ++connect_failed_count;
    });
    client->async_start();

    expect(wait_until([&] { return connect_failed_count.load() >= 1_i; }));
    expect(connected_count.load() == 0_i);

    // After the server appears, the reconnect timer should establish exactly one connection.
    std::atomic_bool server_bound = false;
    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    expect(wait_until([&] { return connected_count.load() == 1_i; }));
    expect(connected_count.load() == 1_i);

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

    auto options = pqrs::unix_domain_stream::options(
        pqrs::unix_domain_stream::options::initialization_parameters{
            .max_message_size = 8,
            .max_send_queue_size = 128,
            .reconnect_interval = std::chrono::milliseconds(100),
            .write_timeout = std::chrono::milliseconds(1000),
        });
    std::atomic_bool server_bound = false;
    std::atomic_bool error_occurred = false;

    // Configure both peers with a small outgoing user data limit.
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

    // Sending more than max_message_size should fail before data reaches the server.
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

    auto options = pqrs::unix_domain_stream::options(
        pqrs::unix_domain_stream::options::initialization_parameters{
            .max_send_queue_size = 128,
            .reconnect_interval = std::chrono::milliseconds(100),
            .server_check_interval = std::chrono::milliseconds(100),
            .write_timeout = std::chrono::milliseconds(1000),
        });

    std::atomic<size_t> bound_count = 0;
    std::atomic<size_t> closed_count = 0;
    std::atomic<size_t> peer_connected_count = 0;
    std::atomic<size_t> client_received_count = 0;
    std::atomic<size_t> verify_peer_count = 0;

    // Server health checks are internal and must not look like normal peers.
    auto server = std::make_unique<pqrs::unix_domain_stream::server>(
        dispatcher,
        server_socket_file_path,
        options,
        [&](auto&&) {
          ++verify_peer_count;
          return true;
        });
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

    expect(wait_until([&] { return bound_count.load() == 1_i; }));

    // Remove the socket file to force the periodic health check to fail and rebind.
    std::error_code error_code;
    std::filesystem::remove(server_socket_file_path, error_code);

    expect(wait_until([&] { return closed_count.load() >= 1_i; }));
    expect(wait_until([&] { return bound_count.load() >= 2_i; }));
    expect(peer_connected_count.load() == 0_i);
    expect(verify_peer_count.load() == 0_i);

    // After rebinding, a real client should still connect and exchange data.
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

    expect(wait_until([&] { return client_received_count.load() == 16_i; }));
    expect(peer_connected_count.load() == 1_i);
    expect(verify_peer_count.load() == 1_i);

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

    auto options = pqrs::unix_domain_stream::options(
        pqrs::unix_domain_stream::options::initialization_parameters{
            .max_send_queue_size = 128,
            .reconnect_interval = std::chrono::milliseconds(100),
            .read_timeout = std::chrono::milliseconds(200),
            .write_timeout = std::chrono::milliseconds(1000),
        });

    std::atomic_bool server_bound = false;
    std::atomic_bool peer_error_occurred = false;
    std::atomic_bool peer_closed = false;

    // Use a raw socket so only the frame header is sent.
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

    // The server should time out waiting for the rest of the declared body.
    expect(wait_until([&] { return peer_error_occurred.load(); }));
    expect(wait_until([&] { return peer_closed.load(); }));

    asio::error_code error_code;
    socket.close(error_code);

    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::malformed_frame"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::malformed_frame)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = pqrs::unix_domain_stream::options(
        pqrs::unix_domain_stream::options::initialization_parameters{
            .max_message_size = 8,
            .max_send_queue_size = 128,
            .reconnect_interval = std::chrono::milliseconds(100),
            .read_timeout = std::chrono::milliseconds(1000),
            .write_timeout = std::chrono::milliseconds(1000),
        });

    std::atomic_bool server_bound = false;
    std::atomic_size_t peer_connected_count = 0;
    std::atomic_size_t peer_closed_count = 0;
    std::atomic_size_t message_size_error_count = 0;
    std::atomic_size_t invalid_argument_error_count = 0;

    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto, auto&&) {
      ++peer_connected_count;
    });
    server->peer_error_occurred.connect([&](auto, auto&& error_code) {
      if (error_code == asio::error::message_size) {
        ++message_size_error_count;
      } else if (error_code == asio::error::invalid_argument) {
        ++invalid_argument_error_count;
      }
    });
    server->peer_closed.connect([&](auto) {
      ++peer_closed_count;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    auto make_raw_frame = [](uint32_t body_size,
                             const std::vector<uint8_t>& body) {
      // Build intentionally malformed frames without using protocol helpers,
      // since those helpers always produce internally consistent frames.
      std::array<uint8_t, pqrs::unix_domain_stream::impl::protocol::header_size> header;
      pqrs::unix_domain_stream::impl::protocol::encode_uint32(header, body_size);

      std::vector<uint8_t> frame;
      frame.insert(frame.end(), header.begin(), header.end());
      frame.insert(frame.end(), body.begin(), body.end());
      return frame;
    };

    struct send_malformed_frame_parameters final {
      std::vector<uint8_t> frame;
      size_t expected_peer_connected_count;
      size_t expected_peer_closed_count;
    };

    auto send_malformed_frame = [&](send_malformed_frame_parameters parameters) {
      // Use a fresh raw connection for each malformed frame. A protocol error
      // closes the peer, so later cases need their own connection.
      asio::io_context io_ctx;
      asio::local::stream_protocol::socket socket(io_ctx);
      socket.connect(asio::local::stream_protocol::endpoint(server_socket_file_path));

      // Wait until the server has exposed this peer. Otherwise an immediate
      // protocol error would be intentionally hidden from public peer signals.
      expect(wait_until([&] { return peer_connected_count.load() == parameters.expected_peer_connected_count; }));

      asio::write(socket, asio::buffer(parameters.frame));

      expect(wait_until([&] { return peer_closed_count.load() == parameters.expected_peer_closed_count; }));

      asio::error_code error_code;
      socket.close(error_code);
    };

    // The declared body size must at least contain the message type byte.
    // Otherwise the receiver cannot even decide how to parse the body.
    send_malformed_frame({
        .frame = make_raw_frame(0, {}),
        .expected_peer_connected_count = 1,
        .expected_peer_closed_count = 1,
    });
    expect(message_size_error_count.load() == 1_i);

    // The declared body size must fit within the configured payload limit.
    // This rejects oversized frames before allocating a matching read buffer.
    send_malformed_frame({
        .frame = make_raw_frame(options.max_message_size +
                                    pqrs::unix_domain_stream::impl::protocol::type_size +
                                    pqrs::unix_domain_stream::impl::protocol::request_id_size +
                                    1,
                                {}),
        .expected_peer_connected_count = 2,
        .expected_peer_closed_count = 2,
    });
    expect(message_size_error_count.load() == 2_i);

    // Request and response frames must include the request_id field.
    // A body containing only the type byte is structurally invalid for them.
    send_malformed_frame({
        .frame = make_raw_frame(
            pqrs::unix_domain_stream::impl::protocol::type_size,
            {static_cast<uint8_t>(pqrs::unix_domain_stream::impl::protocol::message_type::request)}),
        .expected_peer_connected_count = 3,
        .expected_peer_closed_count = 3,
    });
    expect(message_size_error_count.load() == 3_i);

    send_malformed_frame({
        .frame = make_raw_frame(
            pqrs::unix_domain_stream::impl::protocol::type_size,
            {static_cast<uint8_t>(pqrs::unix_domain_stream::impl::protocol::message_type::response)}),
        .expected_peer_connected_count = 4,
        .expected_peer_closed_count = 4,
    });
    expect(message_size_error_count.load() == 4_i);

    // Unknown message types are protocol errors.
    // They should not be ignored and treated as an empty valid frame.
    send_malformed_frame({
        .frame = make_raw_frame(1, {0xff}),
        .expected_peer_connected_count = 5,
        .expected_peer_closed_count = 5,
    });
    expect(invalid_argument_error_count.load() == 1_i);

    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::heartbeat_timeout"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::heartbeat_timeout)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = pqrs::unix_domain_stream::options(
        pqrs::unix_domain_stream::options::initialization_parameters{
            .max_send_queue_size = 128,
            .reconnect_interval = std::chrono::milliseconds(100),
            .heartbeat_interval = std::chrono::milliseconds(1000),
            .heartbeat_timeout = std::chrono::milliseconds(300),
            .read_timeout = std::chrono::milliseconds(1000),
            .write_timeout = std::chrono::milliseconds(1000),
        });

    std::atomic_bool server_bound = false;
    std::atomic_bool peer_connected = false;
    std::atomic_bool peer_error_occurred = false;
    std::atomic_bool peer_closed = false;

    // Connect a raw socket that never sends any frames or heartbeats.
    auto server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                     server_socket_file_path,
                                                                     options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto, auto&&) {
      peer_connected = true;
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

    expect(wait_until([&] { return peer_connected.load(); }));
    // Once the peer is considered ready, missing heartbeats should close it.
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

    auto options = pqrs::unix_domain_stream::options(
        pqrs::unix_domain_stream::options::initialization_parameters{
            .max_send_queue_size = 128,
            .reconnect_interval = std::chrono::milliseconds(100),
            .heartbeat_interval = std::chrono::milliseconds(50),
            .heartbeat_timeout = std::chrono::milliseconds(300),
            .write_timeout = std::chrono::milliseconds(1000),
        });

    std::atomic<size_t> server_received_count = 0;
    std::atomic<size_t> client_received_count = 0;
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;

    // Echo user data while both sides are also exchanging heartbeat frames.
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

    // Let several heartbeat intervals pass before sending user data.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    client->async_send(std::vector<uint8_t>(16, 42));

    expect(wait_until([&] { return client_received_count.load() == 16_i; }));
    expect(server_received_count.load() == 16_i);
    expect(client_received_count.load() == 16_i);

    client = nullptr;
    server = nullptr;

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  return 0;
}
