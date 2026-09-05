#include <array>
#include <asio/local/connect_pair.hpp>
#include <atomic>
#include <boost/ut.hpp>
#include <functional>
#include <future>
#include <iostream>
#include <pqrs/unix_domain_stream.hpp>
#include <pqrs/unix_domain_stream/impl/protocol.hpp>
#include <thread>
#include <unistd.h>
#include <utility>

namespace pqrs::unix_domain_stream::impl {

class runtime_test_access final {
public:
  [[nodiscard]] static asio::io_context& get_io_context() {
    return runtime::get_io_context();
  }

  // Call on the I/O runtime thread.
  [[nodiscard]] static size_t socket_file_path_owner_count() {
    return runtime::get_instance().socket_file_path_owners_.size();
  }
};

class server_test_access final {
public:
  // Call on the I/O runtime thread.
  static std::weak_ptr<peer> get_peer(server_state& server, peer_id id) {
    return make_weak(server.peers_.at(id));
  }

  // Call on the I/O runtime thread.
  static std::weak_ptr<peer> get_first_peer(server_state& server) {
    if (server.peers_.empty()) {
      return {};
    }
    return make_weak(server.peers_.begin()->second);
  }

  // Call on the I/O runtime thread.
  static void close_peer(server_state& server, peer_id id) {
    server.close_peer(id);
  }

  // Call on the dispatcher thread.
  static bool has_exposed_peer(server_state& server, peer_id id) {
    return server.exposed_peer_ids_.contains(id);
  }
};

} // namespace pqrs::unix_domain_stream::impl

namespace {

const std::filesystem::path server_socket_file_path =
    std::filesystem::path("tmp") /
    ("server-" + std::to_string(::getpid()) + ".sock");

using async_request_test_result = std::pair<asio::error_code, std::shared_ptr<std::vector<uint8_t>>>;

struct test_options final {
  struct initialization_parameters final {
    pqrs::unix_domain_stream::common_options::initialization_parameters common;
    pqrs::unix_domain_stream::client_options::initialization_parameters client;
    pqrs::unix_domain_stream::server_options::initialization_parameters server;
  };

  test_options() : client(),
                   server() {
  }

  explicit test_options(const initialization_parameters& parameters)
      : client(parameters.common,
               parameters.client),
        server(parameters.common,
               parameters.server) {
  }

  static initialization_parameters make_parameters(
      const pqrs::unix_domain_stream::common_options::initialization_parameters& common,
      const pqrs::unix_domain_stream::client_options::initialization_parameters& client,
      const pqrs::unix_domain_stream::server_options::initialization_parameters& server) {
    return {
        .common = common,
        .client = client,
        .server = server,
    };
  }

  operator const pqrs::unix_domain_stream::client_options&() const {
    return client;
  }

  operator const pqrs::unix_domain_stream::server_options&() const {
    return server;
  }

  pqrs::unix_domain_stream::client_options client;
  pqrs::unix_domain_stream::server_options server;
};

test_options make_options() {
  return test_options(
      test_options::make_parameters(
          {
              .max_send_queue_size = 128,
              .write_timeout = std::chrono::milliseconds(1000),
          },
          {
              .reconnect_interval = std::chrono::milliseconds(100),
          },
          {
              .bind_retry_interval = std::chrono::milliseconds(100),
          }));
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

bool wait_dispatcher_barrier(pqrs::not_null_shared_ptr_t<pqrs::dispatcher::dispatcher> dispatcher,
                             std::chrono::milliseconds timeout = std::chrono::milliseconds(3000)) {
  pqrs::dispatcher::extra::dispatcher_client barrier(dispatcher.get());
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

class test_server final {
public:
  test_server(const test_server&) = delete;

  template <typename... Args>
  explicit test_server(Args&&... args) : server_(std::make_unique<pqrs::unix_domain_stream::server>(std::forward<Args>(args)...)),
                                         server_ptr_(server_.get()) {
  }

  ~test_server() {
    reset();
  }

  pqrs::unix_domain_stream::server* operator->() const {
    return server_ptr_;
  }

  void reset() {
    if (server_) {
      server_.reset();
      server_ptr_ = nullptr;
    }
  }

private:
  std::unique_ptr<pqrs::unix_domain_stream::server> server_;
  pqrs::unix_domain_stream::server* server_ptr_;
};

class test_client final {
public:
  test_client(const test_client&) = delete;

  template <typename... Args>
  explicit test_client(Args&&... args) : client_(std::make_unique<pqrs::unix_domain_stream::client>(std::forward<Args>(args)...)),
                                         client_ptr_(client_.get()) {
  }

  ~test_client() {
    reset();
  }

  pqrs::unix_domain_stream::client* operator->() const {
    return client_ptr_;
  }

  void reset() {
    if (client_) {
      client_.reset();
      client_ptr_ = nullptr;
    }
  }

private:
  std::unique_ptr<pqrs::unix_domain_stream::client> client_;
  pqrs::unix_domain_stream::client* client_ptr_;
};

} // namespace

int main() {
  using namespace boost::ut;
  using namespace boost::ut::literals;

  "unix_domain_stream::options_initialization_parameters"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::options_initialization_parameters)" << std::endl;

    pqrs::unix_domain_stream::common_options::initialization_parameters common_parameters{
        .max_message_size = 123,
        .max_send_queue_size = 456,
        .heartbeat_interval = std::chrono::milliseconds(456),
        .heartbeat_timeout = std::chrono::milliseconds(567),
        .read_timeout = std::chrono::milliseconds(678),
        .write_timeout = std::chrono::milliseconds(890),
        .invalidate_connection_on_request_error = false,
    };

    // Ensure every client initialization parameter is copied into client_options as-is.
    pqrs::unix_domain_stream::client_options client_options(
        common_parameters,
        pqrs::unix_domain_stream::client_options::initialization_parameters{
            .reconnect_interval = std::chrono::milliseconds(789),
        });

    expect(client_options.max_message_size == 123_i);
    expect(client_options.max_send_queue_size == 456_i);
    expect(client_options.reconnect_interval == std::chrono::milliseconds(789));
    expect(client_options.heartbeat_interval == std::chrono::milliseconds(456));
    expect(client_options.heartbeat_timeout == std::chrono::milliseconds(567));
    expect(client_options.read_timeout == std::chrono::milliseconds(678));
    expect(client_options.write_timeout == std::chrono::milliseconds(890));
    expect(client_options.invalidate_connection_on_request_error == false);

    // Ensure every server initialization parameter is copied into server_options as-is.
    pqrs::unix_domain_stream::server_options server_options(
        common_parameters,
        pqrs::unix_domain_stream::server_options::initialization_parameters{
            .bind_retry_interval = std::chrono::milliseconds(789),
            .socket_path_health_check_interval = std::chrono::milliseconds(234),
            .socket_path_health_check_timeout = std::chrono::milliseconds(345),
        });

    expect(server_options.max_message_size == 123_i);
    expect(server_options.max_send_queue_size == 456_i);
    expect(server_options.bind_retry_interval == std::chrono::milliseconds(789));
    expect(server_options.socket_path_health_check_interval == std::chrono::milliseconds(234));
    expect(server_options.socket_path_health_check_timeout == std::chrono::milliseconds(345));
    expect(server_options.heartbeat_interval == std::chrono::milliseconds(456));
    expect(server_options.heartbeat_timeout == std::chrono::milliseconds(567));
    expect(server_options.read_timeout == std::chrono::milliseconds(678));
    expect(server_options.write_timeout == std::chrono::milliseconds(890));
    expect(server_options.invalidate_connection_on_request_error == false);
  };

  "unix_domain_stream::destruction_does_not_wait_for_io_context"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::destruction_does_not_wait_for_io_context)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    auto io_context_is_blocked_promise = std::make_shared<std::promise<void>>();
    auto io_context_is_blocked_future = io_context_is_blocked_promise->get_future();
    auto release_io_context_promise = std::make_shared<std::promise<void>>();
    auto release_io_context_future = release_io_context_promise->get_future().share();

    asio::post(
        pqrs::unix_domain_stream::impl::runtime_test_access::get_io_context(),
        [io_context_is_blocked_promise, release_io_context_future] {
          io_context_is_blocked_promise->set_value();
          release_io_context_future.wait();
        });

    if (io_context_is_blocked_future.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
      expect(false);
      release_io_context_promise->set_value();
      return;
    }

    auto destruction_future = std::async(std::launch::async, [dispatcher] {
      pqrs::unix_domain_stream::client client(dispatcher,
                                              server_socket_file_path,
                                              make_options());
      pqrs::unix_domain_stream::server server(dispatcher,
                                              server_socket_file_path,
                                              make_options());
    });

    // Destruction must complete while the I/O runtime remains blocked. Check it
    // from this thread so a regression does not deadlock the test process.
    auto destruction_status = destruction_future.wait_for(std::chrono::milliseconds(500));

    release_io_context_promise->set_value();

    expect(destruction_status == std::future_status::ready);
    destruction_future.get();

    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    asio::post(
        pqrs::unix_domain_stream::impl::runtime_test_access::get_io_context(),
        [promise] {
          promise->set_value();
        });
    expect(future.wait_for(std::chrono::seconds(3)) == std::future_status::ready);

    // The shutdown handlers above enqueue one final lifetime barrier. Wait for
    // that barrier as well before terminating their dispatcher.
    auto lifetime_barrier_promise = std::make_shared<std::promise<void>>();
    auto lifetime_barrier_future = lifetime_barrier_promise->get_future();
    asio::post(
        pqrs::unix_domain_stream::impl::runtime_test_access::get_io_context(),
        [lifetime_barrier_promise] {
          lifetime_barrier_promise->set_value();
        });
    expect(lifetime_barrier_future.wait_for(std::chrono::seconds(3)) == std::future_status::ready);

    dispatcher->terminate();
  };

  "unix_domain_stream::server_cleanup_uses_bound_socket_path"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_cleanup_uses_bound_socket_path)" << std::endl;

    // Removing or retargeting the parent symlink must not orphan the old socket.
    for (bool retarget : {false, true}) {
      auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
      auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);
      auto root = server_socket_file_path.parent_path() / ("path-owner-" + std::to_string(::getpid()));
      auto old_path = root / "old" / "server.sock";
      auto new_path = root / "new" / "server.sock";
      auto link = root / "link";
      std::filesystem::create_directories(old_path.parent_path());
      std::filesystem::create_directories(new_path.parent_path());
      std::filesystem::create_directory_symlink(std::filesystem::absolute(old_path.parent_path()), link);

      // This also acts as an I/O barrier after asynchronous server destruction.
      auto owner_count = [] {
        std::promise<size_t> result;
        asio::post(pqrs::unix_domain_stream::impl::runtime_test_access::get_io_context(), [&] {
          result.set_value(pqrs::unix_domain_stream::impl::runtime_test_access::socket_file_path_owner_count());
        });
        return result.get_future().get();
      };
      auto baseline = owner_count();
      auto options = pqrs::unix_domain_stream::server_options(
          {}, {.socket_path_health_check_interval = std::chrono::hours(1)});
      auto old_server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher, link / "server.sock", options);
      std::promise<void> old_bound;
      old_server->bound.connect([&] { old_bound.set_value(); });
      old_server->async_start();
      expect(old_bound.get_future().wait_for(std::chrono::seconds(3)) == std::future_status::ready);
      expect(std::filesystem::exists(old_path));
      expect(owner_count() == baseline + 1);

      // A new server at the retargeted path must survive the old server's cleanup.
      std::filesystem::remove(link);
      std::unique_ptr<pqrs::unix_domain_stream::server> new_server;
      std::promise<void> new_bound;
      if (retarget) {
        std::filesystem::create_directory_symlink(std::filesystem::absolute(new_path.parent_path()), link);
        new_server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher, link / "server.sock", options);
        new_server->bound.connect([&] { new_bound.set_value(); });
        new_server->async_start();
        expect(new_bound.get_future().wait_for(std::chrono::seconds(3)) == std::future_status::ready);
      }
      old_server.reset();
      expect(owner_count() == baseline + (retarget ? 1 : 0));
      expect(!std::filesystem::exists(old_path));
      if (retarget) {
        expect(std::filesystem::exists(new_path));
        asio::io_context io_ctx;
        asio::local::stream_protocol::socket socket(io_ctx);
        asio::error_code error_code;
        socket.connect(asio::local::stream_protocol::endpoint(link / "server.sock"), error_code);
        expect(!error_code);
        socket.close();
      }

      // Destruction must release both the file and the runtime ownership entry.
      new_server.reset();
      expect(owner_count() == baseline);
      expect(!std::filesystem::exists(new_path));
      dispatcher->terminate();
      std::filesystem::remove_all(root);
    }
  };

  "unix_domain_stream::old_server_shutdown_does_not_remove_new_server_socket_path"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::old_server_shutdown_does_not_remove_new_server_socket_path)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    std::atomic_bool old_server_bound = false;
    auto old_server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                         server_socket_file_path,
                                                                         make_options());
    old_server->bound.connect([&old_server_bound] {
      old_server_bound = true;
    });
    old_server->async_start();
    expect(wait_until([&old_server_bound] { return old_server_bound.load(); }));

    auto io_context_is_blocked_promise = std::make_shared<std::promise<void>>();
    auto io_context_is_blocked_future = io_context_is_blocked_promise->get_future();
    auto release_io_context_promise = std::make_shared<std::promise<void>>();
    auto release_io_context_future = release_io_context_promise->get_future().share();

    asio::post(
        pqrs::unix_domain_stream::impl::runtime_test_access::get_io_context(),
        [io_context_is_blocked_promise, release_io_context_future] {
          io_context_is_blocked_promise->set_value();
          release_io_context_future.wait();
        });
    expect(io_context_is_blocked_future.wait_for(std::chrono::seconds(3)) == std::future_status::ready);

    std::atomic_bool new_server_bound = false;
    auto equivalent_server_socket_file_path = server_socket_file_path.parent_path() /
                                              "." /
                                              server_socket_file_path.filename();
    expect(equivalent_server_socket_file_path != server_socket_file_path);
    expect(equivalent_server_socket_file_path.lexically_normal() == server_socket_file_path);

    auto new_server = std::make_unique<pqrs::unix_domain_stream::server>(dispatcher,
                                                                         equivalent_server_socket_file_path,
                                                                         make_options());
    new_server->bound.connect([&new_server_bound] {
      new_server_bound = true;
    });
    new_server->async_start();

    // Ensure the new bind is queued before the old server queues its delayed
    // shutdown on the blocked I/O runtime.
    expect(wait_dispatcher_barrier(dispatcher));
    old_server.reset();
    release_io_context_promise->set_value();

    expect(wait_until([&new_server_bound] { return new_server_bound.load(); }));

    auto io_context_barrier_promise = std::make_shared<std::promise<void>>();
    auto io_context_barrier_future = io_context_barrier_promise->get_future();
    asio::post(
        pqrs::unix_domain_stream::impl::runtime_test_access::get_io_context(),
        [io_context_barrier_promise] {
          io_context_barrier_promise->set_value();
        });
    expect(io_context_barrier_future.wait_for(std::chrono::seconds(3)) == std::future_status::ready);
    expect(std::filesystem::exists(server_socket_file_path));

    std::atomic_bool client_connected = false;
    auto client = std::make_unique<pqrs::unix_domain_stream::client>(dispatcher,
                                                                     equivalent_server_socket_file_path,
                                                                     make_options());
    client->connected.connect([&client_connected](auto&&) {
      client_connected = true;
    });
    client->async_start();
    expect(wait_until([&client_connected] { return client_connected.load(); }));

    client.reset();
    new_server.reset();
    dispatcher->terminate();
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
    test_server server(dispatcher,
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

    test_client client(dispatcher,
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
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::client_close_is_not_peer_error"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::client_close_is_not_peer_error)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    std::atomic_bool server_bound = false;
    std::atomic_bool peer_connected = false;
    std::atomic_bool client_connected = false;
    std::atomic_bool peer_closed = false;
    std::atomic_bool peer_error_occurred = false;

    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto, auto&&) {
      peer_connected = true;
    });
    server->peer_closed.connect([&](auto) {
      peer_closed = true;
    });
    server->peer_error_occurred.connect([&](auto, auto&&) {
      peer_error_occurred = true;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));
    expect(wait_until([&] { return peer_connected.load(); }));

    client.reset();

    expect(wait_until([&] { return peer_closed.load(); }));
    expect(wait_dispatcher_barrier(dispatcher));
    expect(!peer_error_occurred.load());

    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_destroyed_while_write_queue_is_active"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_destroyed_while_write_queue_is_active)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    // Read server responses with small client-side buffers while the server
    // queues many writes, then destroy the server before the response path
    // drains. This increases the chance that write completions and peer close
    // overlap under ASan.
    constexpr size_t socket_count = 64;
    constexpr size_t send_count_per_peer = 256;
    constexpr size_t payload_size = 16384;
    auto options = test_options(test_options::make_parameters(
        {
            .max_message_size = payload_size,
            .max_send_queue_size = send_count_per_peer + 1,
            .read_timeout = std::chrono::milliseconds(3000),
            .write_timeout = std::chrono::milliseconds(3000),
        },
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
            .socket_path_health_check_interval = std::chrono::milliseconds(60000),
        }));

    std::atomic_bool server_bound = false;
    std::atomic_size_t peer_connected_count = 0;
    std::atomic_size_t server_received_count = 0;
    std::atomic_size_t client_received_count = 0;
    auto outbound_payload = std::make_shared<std::vector<uint8_t>>(payload_size, 42);

    struct active_write_server final {
      active_write_server(pqrs::not_null_shared_ptr_t<pqrs::dispatcher::dispatcher> dispatcher,
                          const test_options& options,
                          std::atomic_bool& server_bound,
                          std::atomic_size_t& peer_connected_count,
                          std::atomic_size_t& server_received_count,
                          size_t send_count_per_peer,
                          pqrs::not_null_shared_ptr_t<std::vector<uint8_t>> outbound_payload)
          : server_(std::make_unique<pqrs::unix_domain_stream::server>(dispatcher.get(),
                                                                       server_socket_file_path,
                                                                       options)),
            server_ptr_(server_.get()),
            server_bound_(server_bound),
            peer_connected_count_(peer_connected_count),
            server_received_count_(server_received_count),
            send_count_per_peer_(send_count_per_peer),
            outbound_payload_(std::move(outbound_payload)) {
        server_->bound.connect([this] {
          server_bound_ = true;
        });
        server_->peer_connected.connect([this](auto, auto&&) {
          ++peer_connected_count_;
        });
        server_->received.connect([this](auto peer_id, auto&& buffer) {
          for (size_t i = 0; i < send_count_per_peer_; ++i) {
            server_ptr_->async_send(peer_id, *outbound_payload_);
          }
          server_received_count_ += buffer->size();
        });
      }

      ~active_write_server() {
        server_.reset();
      }

      void async_start() {
        server_ptr_->async_start();
      }

    private:
      std::unique_ptr<pqrs::unix_domain_stream::server> server_;
      pqrs::unix_domain_stream::server* server_ptr_;
      std::atomic_bool& server_bound_;
      std::atomic_size_t& peer_connected_count_;
      std::atomic_size_t& server_received_count_;
      size_t send_count_per_peer_;
      pqrs::not_null_shared_ptr_t<std::vector<uint8_t>> outbound_payload_;
    };

    auto server = std::make_unique<active_write_server>(dispatcher,
                                                        options,
                                                        server_bound,
                                                        peer_connected_count,
                                                        server_received_count,
                                                        send_count_per_peer,
                                                        outbound_payload);
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    asio::io_context io_ctx;
    auto work_guard = asio::make_work_guard(io_ctx);
    std::vector<std::unique_ptr<asio::local::stream_protocol::socket>> sockets;
    sockets.reserve(socket_count);

    for (size_t i = 0; i < socket_count; ++i) {
      auto socket = std::make_unique<asio::local::stream_protocol::socket>(io_ctx);
      socket->connect(asio::local::stream_protocol::endpoint(server_socket_file_path));
      sockets.push_back(std::move(socket));
    }

    std::atomic_bool keep_reading = true;
    std::vector<std::array<uint8_t, 4096>> read_buffers(socket_count);
    std::function<void(size_t)> start_read = [&](size_t index) {
      if (!keep_reading.load()) {
        return;
      }

      sockets[index]->async_read_some(
          asio::buffer(read_buffers[index]),
          [&, index](auto&& error_code, auto bytes_transferred) {
            if (!error_code) {
              client_received_count += bytes_transferred;
              start_read(index);
            }
          });
    };

    for (size_t i = 0; i < socket_count; ++i) {
      start_read(i);
    }

    std::thread io_ctx_thread([&] {
      io_ctx.run();
    });

    auto user_data_frame = pqrs::unix_domain_stream::impl::protocol::make_user_data_frame(std::vector<uint8_t>{1});
    for (auto& socket : sockets) {
      asio::write(*socket, asio::buffer(user_data_frame));
    }

    expect(wait_until([&] { return peer_connected_count.load() == socket_count; },
                      std::chrono::milliseconds(10000)));
    expect(wait_until([&] { return server_received_count.load() == socket_count; },
                      std::chrono::milliseconds(10000)));
    expect(wait_until([&] { return client_received_count.load() > 0; },
                      std::chrono::milliseconds(10000)));
    server.reset();

    keep_reading = false;
    for (auto& socket : sockets) {
      asio::error_code error_code;
      socket->close(error_code);
    }

    work_guard.reset();
    if (io_ctx_thread.joinable()) {
      io_ctx_thread.join();
    }

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
    auto options = test_options(test_options::make_parameters(
        {
            .max_send_queue_size = 128,
            .write_timeout = std::chrono::milliseconds(1000),
        },
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
            .socket_path_health_check_interval = std::chrono::milliseconds(60000),
        }));
    std::atomic<pqrs::unix_domain_stream::peer_id> connected_peer_id = 0;
    std::atomic<size_t> server_received_count = 0;
    std::atomic<size_t> client_received_count = 0;
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;

    test_server server(dispatcher,
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

    test_client client(dispatcher,
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
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_removes_stale_socket_file_on_start"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_removes_stale_socket_file_on_start)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    // Leave behind a socket pathname without a listening server.
    asio::io_context io_ctx;
    asio::local::stream_protocol::acceptor stale_acceptor(io_ctx);
    stale_acceptor.open(asio::local::stream_protocol::endpoint(server_socket_file_path).protocol());
    stale_acceptor.bind(asio::local::stream_protocol::endpoint(server_socket_file_path));
    stale_acceptor.listen();
    stale_acceptor.close();
    expect(std::filesystem::exists(server_socket_file_path));

    asio::local::stream_protocol::socket stale_client_socket(io_ctx);
    asio::error_code stale_connect_error_code;
    stale_client_socket.connect(asio::local::stream_protocol::endpoint(server_socket_file_path),
                                stale_connect_error_code);
    expect(static_cast<bool>(stale_connect_error_code));

    auto options = make_options();
    std::atomic_bool stale_client_connected = false;
    std::atomic_bool stale_client_connect_failed = false;
    test_client stale_client(dispatcher,
                             server_socket_file_path,
                             options);
    stale_client->connected.connect([&](auto&&) {
      stale_client_connected = true;
    });
    stale_client->connect_failed.connect([&](auto&&) {
      stale_client_connect_failed = true;
    });
    stale_client->async_start();
    expect(wait_until([&] { return stale_client_connect_failed.load(); }));
    expect(!stale_client_connected.load());
    stale_client.reset();

    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;

    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));
    expect(std::filesystem::exists(server_socket_file_path));

    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));

    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_bind_retry_interval"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_bind_retry_interval)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    const auto missing_parent_socket_file_path = std::filesystem::path("tmp/missing-parent/server.sock");
    std::error_code error_code;
    std::filesystem::remove_all(missing_parent_socket_file_path.parent_path(), error_code);

    auto options = test_options(test_options::make_parameters(
        {},
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
            .socket_path_health_check_interval = std::chrono::milliseconds(60000),
        }));

    std::atomic_size_t bind_failed_count = 0;
    test_server server(dispatcher,
                       missing_parent_socket_file_path,
                       options);
    server->bind_failed.connect([&](auto&& error_code) {
      expect(static_cast<bool>(error_code));
      ++bind_failed_count;
    });
    server->async_start();

    expect(wait_until([&] { return bind_failed_count.load() >= 3_i; },
                      std::chrono::milliseconds(1000)));

    // A missing parent directory should retry at bind_retry_interval pace, not
    // spin in a tight bind_failed loop.
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    expect(wait_dispatcher_barrier(dispatcher));
    expect(bind_failed_count.load() <= 8_i);

    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_bind_retry_zero_interval"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_bind_retry_zero_interval)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    const auto missing_parent_socket_file_path = std::filesystem::path("tmp/missing-parent/server.sock");
    std::error_code error_code;
    std::filesystem::remove_all(missing_parent_socket_file_path.parent_path(), error_code);

    auto options = test_options(test_options::make_parameters(
        {},
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(0),
            .socket_path_health_check_interval = std::chrono::milliseconds(60000),
        }));

    std::atomic_size_t bind_failed_count = 0;
    test_server server(dispatcher,
                       missing_parent_socket_file_path,
                       options);
    server->bind_failed.connect([&](auto&& error_code) {
      expect(static_cast<bool>(error_code));
      ++bind_failed_count;
    });
    server->async_start();

    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    expect(wait_dispatcher_barrier(dispatcher));
    expect(bind_failed_count.load() > 0_i);
    expect(bind_failed_count.load() <= 5_i);

    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::verify_peer_runs_on_dispatcher"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::verify_peer_runs_on_dispatcher)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = test_options(test_options::make_parameters(
        {},
        {
            .reconnect_interval = std::chrono::milliseconds(1000),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(1000),
            .socket_path_health_check_interval = std::chrono::milliseconds(60000),
        }));

    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;
    std::atomic_bool verify_peer_called = false;
    std::promise<std::thread::id> verify_peer_thread_id_promise;
    std::promise<std::thread::id> peer_connected_thread_id_promise;

    // Capture the dispatcher thread used by verify_peer and peer_connected.
    test_server server(
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

    test_client client(dispatcher,
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
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_drops_unverified_peer_data"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_drops_unverified_peer_data)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = test_options(test_options::make_parameters(
        {
            .max_send_queue_size = 128,
            .read_timeout = std::chrono::milliseconds(1000),
            .write_timeout = std::chrono::milliseconds(1000),
        },
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
            .socket_path_health_check_interval = std::chrono::milliseconds(60000),
        }));

    std::atomic_bool server_bound = false;
    std::atomic_size_t verify_peer_count = 0;
    std::atomic_size_t peer_connected_count = 0;
    std::atomic_size_t server_received_count = 0;
    std::atomic_size_t server_request_received_count = 0;

    // Reject the first peer and accept the second peer, without depending on
    // platform-specific peer_credentials values.
    test_server server(
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
    server.reset();

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
    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    test_client client(
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
    client.reset();
    server.reset();

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
                                                                       options.client);
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

  "unix_domain_stream::peer_close_cancels_timers_when_socket_is_closed"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::peer_close_cancels_timers_when_socket_is_closed)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    asio::io_context io_ctx;
    auto work_guard = asio::make_work_guard(io_ctx);
    std::thread io_ctx_thread([&] {
      io_ctx.run();
    });

    auto options = make_options();
    options.client.heartbeat_interval = std::chrono::milliseconds(1000);
    options.client.heartbeat_timeout = std::chrono::milliseconds(1000);

    // A default-constructed socket is already closed. async_start still arms
    // the peer timers, so async_close must cancel them even though there is no
    // socket to close.
    asio::local::stream_protocol::socket socket(io_ctx);
    auto peer = std::make_shared<pqrs::unix_domain_stream::impl::peer>(dispatcher,
                                                                       std::move(socket),
                                                                       options.client);
    std::weak_ptr<pqrs::unix_domain_stream::impl::peer> weak_peer(peer);

    peer->async_start();
    peer->async_close();
    peer.reset();

    expect(wait_until([&] { return weak_peer.expired(); },
                      std::chrono::milliseconds(300)));

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

    test_server server(dispatcher,
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

    test_client client(dispatcher,
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
    client.reset();
    server.reset();

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
    std::atomic_size_t client_connected_count = 0;
    std::atomic_size_t client_closed_count = 0;
    std::atomic_size_t server_request_received_count = 0;

    // Accept the request on the server, but intentionally do not respond.
    test_server server(dispatcher,
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

    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connected.connect([&](auto&&) {
      ++client_connected_count;
    });
    client->closed.connect([&] {
      ++client_closed_count;
    });
    client->async_start();
    expect(wait_until([&] { return client_connected_count.load() == 1_i; }));

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

    // By default, a request timeout invalidates the connection and reconnects.
    expect(wait_until([&] { return client_closed_count.load() == 1_i; }));
    expect(wait_until([&] { return client_connected_count.load() == 2_i; }));
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::client_async_request_timeout_keeps_connection_when_configured"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::client_async_request_timeout_keeps_connection_when_configured)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto parameters = test_options::make_parameters(
        {
            .max_send_queue_size = 128,
            .write_timeout = std::chrono::milliseconds(1000),
            .invalidate_connection_on_request_error = false,
        },
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
            .socket_path_health_check_interval = std::chrono::milliseconds(60000),
        });
    auto options = test_options(parameters);

    std::atomic_bool server_bound = false;
    std::atomic_size_t client_connected_count = 0;
    std::atomic_size_t client_closed_count = 0;
    std::atomic_size_t client_received_count = 0;
    std::atomic_size_t server_request_received_count = 0;

    // Accept the request without responding, but echo normal user data.
    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->request_received.connect([&](auto, auto, auto&&) {
      ++server_request_received_count;
    });
    server->received.connect([&](auto peer_id, auto&& buffer) {
      server->async_send(peer_id, *buffer);
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connected.connect([&](auto&&) {
      ++client_connected_count;
    });
    client->closed.connect([&] {
      ++client_closed_count;
    });
    client->received.connect([&](auto&& buffer) {
      client_received_count += buffer->size();
    });
    client->async_start();
    expect(wait_until([&] { return client_connected_count.load() == 1_i; }));

    std::promise<async_request_test_result> promise;
    auto future = promise.get_future();

    client->async_request(std::vector<uint8_t>{1},
                          std::chrono::milliseconds(100),
                          [&promise](const auto& error_code, auto response) {
                            promise.set_value({error_code, response});
                          });

    expect(future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto [error_code, response] = future.get();
    expect(error_code == asio::error::timed_out);
    expect(response == nullptr);
    expect(server_request_received_count.load() == 1_i);

    // With invalidate_connection_on_request_error disabled, only the request
    // fails; the underlying stream must remain connected and usable.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    expect(wait_dispatcher_barrier(dispatcher));
    expect(client_closed_count.load() == 0_i);
    expect(client_connected_count.load() == 1_i);

    client->async_send(std::vector<uint8_t>(8, 42));
    expect(wait_until([&] { return client_received_count.load() == 8_i; }));

    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::client_async_request_not_connected"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::client_async_request_not_connected)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();

    test_client client(dispatcher,
                       server_socket_file_path,
                       options);

    std::promise<async_request_test_result> promise;
    auto future = promise.get_future();

    // Without an active peer, async_request should fail immediately instead of
    // queueing the request for a future reconnect.
    client->async_request(std::vector<uint8_t>{1},
                          [&promise](const auto& error_code, auto response) {
                            promise.set_value({error_code, response});
                          });

    expect(future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto [error_code, response] = future.get();
    expect(error_code == asio::error::not_connected);
    expect(response == nullptr);

    client.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_async_request"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_async_request)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;
    std::atomic<pqrs::unix_domain_stream::peer_id> connected_peer_id = 0;

    // Start the server and remember the peer id assigned to the client.
    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto peer_id, auto&&) {
      connected_peer_id = peer_id;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    pqrs::unix_domain_stream::request_id first_request_id = 0;

    // Connect one client and make it respond to the two requests out of order.
    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    // Delay the response to request 1 until request 2 arrives, to confirm
    // server-side responses are matched by request_id rather than completion order.
    client->request_received.connect([&](auto request_id, auto&& buffer) {
      if (buffer->at(0) == 1) {
        first_request_id = request_id;
        return;
      }

      client->async_respond(request_id, std::vector<uint8_t>{20});
      client->async_respond(first_request_id, std::vector<uint8_t>{10});
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));
    expect(wait_until([&] { return connected_peer_id.load() != 0_i; }));

    std::promise<async_request_test_result> promise1;
    std::promise<async_request_test_result> promise2;
    auto future1 = promise1.get_future();
    auto future2 = promise2.get_future();

    // Send two concurrent server-side requests to the same peer.
    server->async_request(connected_peer_id,
                          std::vector<uint8_t>{1},
                          [&promise1](const auto& error_code, auto response) {
                            promise1.set_value({error_code, response});
                          });
    server->async_request(connected_peer_id,
                          std::vector<uint8_t>{2},
                          [&promise2](const auto& error_code, auto response) {
                            promise2.set_value({error_code, response});
                          });

    // Both callbacks should complete even though the responses arrive in reverse order.
    expect(future1.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);
    expect(future2.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto [error_code1, response1] = future1.get();
    auto [error_code2, response2] = future2.get();

    expect(!error_code1);
    expect(!error_code2);
    expect(response1 != nullptr);
    expect(response2 != nullptr);
    // Each callback should receive the response for its own request_id.
    expect(*response1 == std::vector<uint8_t>{10});
    expect(*response2 == std::vector<uint8_t>{20});
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_async_request_ignores_response_from_other_peer"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_async_request_ignores_response_from_other_peer)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    std::atomic_bool server_bound = false;
    std::atomic_bool target_client_connected = false;
    std::atomic_bool other_client_connected = false;
    std::atomic<pqrs::unix_domain_stream::peer_id> target_peer_id = 0;
    std::atomic<pqrs::unix_domain_stream::peer_id> other_peer_id = 0;

    // Start the server and remember the two peer ids assigned to the clients.
    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto peer_id, auto&&) {
      if (target_peer_id == 0) {
        target_peer_id = peer_id;
      } else {
        other_peer_id = peer_id;
      }
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    std::atomic_size_t target_request_received_count = 0;
    std::atomic<pqrs::unix_domain_stream::request_id> target_request_id = 0;

    // The target client receives the request but intentionally does not respond.
    test_client target_client(dispatcher,
                              server_socket_file_path,
                              options);
    target_client->connected.connect([&](auto&&) {
      target_client_connected = true;
    });
    target_client->request_received.connect([&](auto request_id, auto&&) {
      target_request_id = request_id;
      ++target_request_received_count;
    });
    target_client->async_start();
    expect(wait_until([&] { return target_client_connected.load(); }));
    expect(wait_until([&] { return target_peer_id.load() != 0_i; }));

    // The other client will try to complete the target client's request_id.
    test_client other_client(dispatcher,
                             server_socket_file_path,
                             options);
    other_client->connected.connect([&](auto&&) {
      other_client_connected = true;
    });
    other_client->async_start();
    expect(wait_until([&] { return other_client_connected.load(); }));
    expect(wait_until([&] { return other_peer_id.load() != 0_i; }));

    std::promise<async_request_test_result> promise;
    auto future = promise.get_future();

    server->async_request(target_peer_id,
                          std::vector<uint8_t>{1},
                          [&promise](const auto& error_code, auto response) {
                            promise.set_value({error_code, response});
                          });

    expect(wait_until([&] { return target_request_received_count.load() == 1_i; }));

    // A response from a different peer with the same request_id must be ignored.
    other_client->async_respond(target_request_id, std::vector<uint8_t>{99});

    auto status = future.wait_for(std::chrono::milliseconds(300));
    expect(status == std::future_status::timeout);

    if (status == std::future_status::ready) {
      future.get();
    } else {
      server->async_close_peer(target_peer_id);
      expect(future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

      auto [error_code, response] = future.get();
      expect(error_code == asio::error::operation_aborted);
      expect(response == nullptr);
    }

    other_client.reset();
    target_client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_async_request_timeout"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_async_request_timeout)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;
    std::atomic_size_t client_closed_count = 0;
    std::atomic_size_t client_request_received_count = 0;
    std::atomic<pqrs::unix_domain_stream::peer_id> connected_peer_id = 0;

    // Start the server and remember the peer id assigned to the client.
    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto peer_id, auto&&) {
      connected_peer_id = peer_id;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    // Connect one client so the server has a target peer for async_request.
    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->closed.connect([&] {
      ++client_closed_count;
    });
    client->request_received.connect([&](auto, auto&&) {
      ++client_request_received_count;
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));
    expect(wait_until([&] { return connected_peer_id.load() != 0_i; }));

    std::promise<async_request_test_result> promise;
    auto future = promise.get_future();

    // Send a server-side request, but do not respond from the client.
    server->async_request(connected_peer_id,
                          std::vector<uint8_t>{1},
                          std::chrono::milliseconds(100),
                          [&promise](const auto& error_code, auto response) {
                            promise.set_value({error_code, response});
                          });

    // The callback should complete when the request timeout expires.
    expect(future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto [error_code, response] = future.get();
    expect(error_code == asio::error::timed_out);
    expect(response == nullptr);
    // The client still received the request before the timeout.
    expect(client_request_received_count.load() == 1_i);
    // By default, a request timeout invalidates the peer connection.
    expect(wait_until([&] { return client_closed_count.load() == 1_i; }));
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_async_request_timeout_keeps_connection_when_configured"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_async_request_timeout_keeps_connection_when_configured)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto parameters = test_options::make_parameters(
        {
            .max_send_queue_size = 128,
            .write_timeout = std::chrono::milliseconds(1000),
            .invalidate_connection_on_request_error = false,
        },
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
            .socket_path_health_check_interval = std::chrono::milliseconds(60000),
        });
    auto options = test_options(parameters);

    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;
    std::atomic_size_t client_closed_count = 0;
    std::atomic_size_t client_request_received_count = 0;
    std::atomic_size_t server_received_count = 0;
    std::atomic<pqrs::unix_domain_stream::peer_id> connected_peer_id = 0;

    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto peer_id, auto&&) {
      connected_peer_id = peer_id;
    });
    server->received.connect([&](auto, auto&& buffer) {
      server_received_count += buffer->size();
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    // Accept the request without responding, but keep normal user data usable.
    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->closed.connect([&] {
      ++client_closed_count;
    });
    client->request_received.connect([&](auto, auto&&) {
      ++client_request_received_count;
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));
    expect(wait_until([&] { return connected_peer_id.load() != 0_i; }));

    std::promise<async_request_test_result> promise;
    auto future = promise.get_future();

    server->async_request(connected_peer_id,
                          std::vector<uint8_t>{1},
                          std::chrono::milliseconds(100),
                          [&promise](const auto& error_code, auto response) {
                            promise.set_value({error_code, response});
                          });

    expect(future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto [error_code, response] = future.get();
    expect(error_code == asio::error::timed_out);
    expect(response == nullptr);
    expect(client_request_received_count.load() == 1_i);

    // With invalidate_connection_on_request_error disabled, only the request
    // fails; the underlying stream must remain connected and usable.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    expect(wait_dispatcher_barrier(dispatcher));
    expect(client_closed_count.load() == 0_i);

    client->async_send(std::vector<uint8_t>(8, 42));
    expect(wait_until([&] { return server_received_count.load() == 8_i; }));

    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_async_request_timeout_closes_only_target_peer"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_async_request_timeout_closes_only_target_peer)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    std::atomic_bool server_bound = false;
    std::atomic_bool target_client_connected = false;
    std::atomic_bool other_client_connected = false;
    std::atomic_size_t target_client_closed_count = 0;
    std::atomic_size_t other_client_closed_count = 0;
    std::atomic_size_t target_request_received_count = 0;
    std::atomic_size_t server_received_count = 0;
    std::atomic<pqrs::unix_domain_stream::peer_id> target_peer_id = 0;
    std::atomic<pqrs::unix_domain_stream::peer_id> other_peer_id = 0;

    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto peer_id, auto&&) {
      if (target_peer_id == 0) {
        target_peer_id = peer_id;
      } else {
        other_peer_id = peer_id;
      }
    });
    server->received.connect([&](auto, auto&& buffer) {
      server_received_count += buffer->size();
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    // Connect the target client and let its request time out without responding.
    test_client target_client(dispatcher,
                              server_socket_file_path,
                              options);
    target_client->connected.connect([&](auto&&) {
      target_client_connected = true;
    });
    target_client->closed.connect([&] {
      ++target_client_closed_count;
    });
    target_client->request_received.connect([&](auto, auto&&) {
      ++target_request_received_count;
    });
    target_client->async_start();
    expect(wait_until([&] { return target_client_connected.load(); }));
    expect(wait_until([&] { return target_peer_id.load() != 0_i; }));

    // Connect another client that should not be affected by the target timeout.
    test_client other_client(dispatcher,
                             server_socket_file_path,
                             options);
    other_client->connected.connect([&](auto&&) {
      other_client_connected = true;
    });
    other_client->closed.connect([&] {
      ++other_client_closed_count;
    });
    other_client->async_start();
    expect(wait_until([&] { return other_client_connected.load(); }));
    expect(wait_until([&] { return other_peer_id.load() != 0_i; }));

    std::promise<async_request_test_result> promise;
    auto future = promise.get_future();

    // Send a server-side request to the target peer, but do not respond from
    // that peer so the request timeout path invalidates the connection.
    server->async_request(target_peer_id,
                          std::vector<uint8_t>{1},
                          std::chrono::milliseconds(100),
                          [&promise](const auto& error_code, auto response) {
                            promise.set_value({error_code, response});
                          });

    expect(future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto [error_code, response] = future.get();
    expect(error_code == asio::error::timed_out);
    expect(response == nullptr);
    expect(target_request_received_count.load() == 1_i);
    // The timed-out target peer should close, while the other peer remains usable.
    expect(wait_until([&] { return target_client_closed_count.load() == 1_i; }));
    expect(other_client_closed_count.load() == 0_i);

    other_client->async_send(std::vector<uint8_t>(8, 42));
    expect(wait_until([&] { return server_received_count.load() == 8_i; }));

    other_client.reset();
    target_client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_async_request_timeout_completes_same_peer_pending_requests"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_async_request_timeout_completes_same_peer_pending_requests)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;
    std::atomic_size_t client_request_received_count = 0;
    std::atomic<pqrs::unix_domain_stream::peer_id> connected_peer_id = 0;

    // Start the server and remember the peer id assigned to the client.
    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto peer_id, auto&&) {
      connected_peer_id = peer_id;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    // Connect one client that receives requests without responding to them.
    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->request_received.connect([&](auto, auto&&) {
      ++client_request_received_count;
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));
    expect(wait_until([&] { return connected_peer_id.load() != 0_i; }));

    std::promise<async_request_test_result> promise1;
    std::promise<async_request_test_result> promise2;
    auto future1 = promise1.get_future();
    auto future2 = promise2.get_future();

    // Send two pending requests to the same peer. The first request times out
    // quickly and invalidates the peer before the second request timeout expires.
    server->async_request(connected_peer_id,
                          std::vector<uint8_t>{1},
                          std::chrono::milliseconds(100),
                          [&promise1](const auto& error_code, auto response) {
                            promise1.set_value({error_code, response});
                          });
    server->async_request(connected_peer_id,
                          std::vector<uint8_t>{2},
                          std::chrono::milliseconds(5000),
                          [&promise2](const auto& error_code, auto response) {
                            promise2.set_value({error_code, response});
                          });

    expect(wait_until([&] { return client_request_received_count.load() == 2_i; }));
    expect(future1.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);
    expect(future2.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto [error_code1, response1] = future1.get();
    auto [error_code2, response2] = future2.get();

    // The timed-out request keeps its timeout error, and the remaining request
    // completes because the peer was closed by the invalidation.
    expect(error_code1 == asio::error::timed_out);
    expect(response1 == nullptr);
    expect(error_code2 == asio::error::operation_aborted);
    expect(response2 == nullptr);

    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_stop_discards_pending_peer_ready"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_stop_discards_pending_peer_ready)" << std::endl;

    // A ready notification from before stop must stay invalid after restart.
    for (bool restart : {false, true}) {
      auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
      auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);
      prepare_socket_file_path(server_socket_file_path);
      std::atomic_size_t verification_count = 0;
      auto server = std::make_shared<pqrs::unix_domain_stream::impl::server_state>(
          dispatcher, server_socket_file_path, make_options().server,
          [&](const auto&) {
            ++verification_count;
            return true;
          });
      std::atomic_size_t bound_count = 0;
      std::atomic_size_t connected_count = 0;
      server->bound.connect([&] { ++bound_count; });
      server->peer_connected.connect([&](auto, const auto&) { ++connected_count; });
      server->async_start();
      expect(wait_until([&] { return bound_count.load() == 1; }));

      // Hold the dispatcher while a real connection queues its ready signal.
      pqrs::dispatcher::extra::dispatcher_client blocker(dispatcher);
      std::promise<void> first_started;
      std::promise<void> first_release;
      auto first_release_future = first_release.get_future().share();
      blocker.enqueue_to_dispatcher([&] {
        first_started.set_value();
        first_release_future.wait();
      });
      first_started.get_future().wait();
      asio::io_context io_ctx;
      asio::local::stream_protocol::socket socket(io_ctx);
      socket.connect(asio::local::stream_protocol::endpoint(server_socket_file_path));
      std::weak_ptr<pqrs::unix_domain_stream::impl::peer> weak_peer;
      expect(wait_until([&] {
        std::promise<std::weak_ptr<pqrs::unix_domain_stream::impl::peer>> result;
        asio::post(pqrs::unix_domain_stream::impl::runtime_test_access::get_io_context(), [&] {
          result.set_value(pqrs::unix_domain_stream::impl::server_test_access::get_first_peer(*server));
        });
        weak_peer = result.get_future().get();
        return !weak_peer.expired();
      }));
      // Allow the peer's 100 ms ready deadline to run on the I/O thread.
      std::promise<void> ready_deadline_passed;
      asio::post(pqrs::unix_domain_stream::impl::runtime_test_access::get_io_context(), [&] {
        auto timer = std::make_shared<asio::steady_timer>(pqrs::unix_domain_stream::impl::runtime_test_access::get_io_context());
        timer->expires_after(std::chrono::milliseconds(200));
        timer->async_wait([timer, &ready_deadline_passed](const auto&) { ready_deadline_passed.set_value(); });
      });
      ready_deadline_passed.get_future().wait();

      // ready first queues the server callback behind stop and this second
      // blocker. Wait for peer destruction before letting that callback run.
      server->async_stop();
      if (restart) {
        server->async_start();
      }
      std::promise<void> second_started;
      std::promise<void> second_release;
      auto second_release_future = second_release.get_future().share();
      blocker.enqueue_to_dispatcher([&] {
        second_started.set_value();
        second_release_future.wait();
      });
      first_release.set_value();
      second_started.get_future().wait();
      expect(wait_until([&] { return weak_peer.expired(); }));
      second_release.set_value();
      blocker.detach_from_dispatcher();

      // No verification, connection notification, or exposed ID may survive.
      std::promise<bool> exposed;
      server->enqueue_to_dispatcher([&] {
        exposed.set_value(pqrs::unix_domain_stream::impl::server_test_access::has_exposed_peer(*server, 1));
      });
      expect(!exposed.get_future().get());
      expect(verification_count.load() == 0);
      expect(connected_count.load() == 0);

      // A new connection after restart must still be verified and reported.
      if (restart) {
        expect(wait_until([&] { return bound_count.load() == 2; }));
        asio::local::stream_protocol::socket new_socket(io_ctx);
        new_socket.connect(asio::local::stream_protocol::endpoint(server_socket_file_path));
        expect(wait_until([&] { return connected_count.load() == 1; }));
        expect(verification_count.load() == 1);
        new_socket.close();
      }
      socket.close();
      server->async_shutdown();
      server.reset();
      dispatcher->terminate();
    }
  };

  "unix_domain_stream::server_local_close_cleans_up_before_peer_notification"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_local_close_cleans_up_before_peer_notification)" << std::endl;

    // Both explicit close and request timeout must clean up even when the
    // dispatcher cannot deliver the peer's closed signal before destruction.
    for (bool request_timeout : {false, true}) {
      // Start the server and connect a raw socket that leaves requests unanswered.
      auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
      auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);
      prepare_socket_file_path(server_socket_file_path);
      auto server = std::make_shared<pqrs::unix_domain_stream::impl::server_state>(
          dispatcher, server_socket_file_path, make_options().server,
          pqrs::unix_domain_stream::default_verify_peer);
      std::promise<void> bound;
      server->bound.connect([&] { bound.set_value(); });
      server->async_start();
      expect(bound.get_future().wait_for(std::chrono::seconds(3)) == std::future_status::ready);

      std::promise<pqrs::unix_domain_stream::peer_id> connected;
      server->peer_connected.connect([&](auto id, const auto&) { connected.set_value(id); });
      asio::io_context io_ctx;
      asio::local::stream_protocol::socket socket(io_ctx);
      socket.connect(asio::local::stream_protocol::endpoint(server_socket_file_path));
      auto id = connected.get_future().get();
      std::atomic_size_t closed_count = 0;
      server->peer_closed.connect([&](auto closed_id) {
        expect(closed_id == id);
        expect(dispatcher->dispatcher_thread());
        ++closed_count;
      });

      // Observe peer destruction without extending its lifetime.
      std::promise<std::weak_ptr<pqrs::unix_domain_stream::impl::peer>> peer_promise;
      asio::post(pqrs::unix_domain_stream::impl::runtime_test_access::get_io_context(), [&] {
        peer_promise.set_value(pqrs::unix_domain_stream::impl::server_test_access::get_peer(*server, id));
      });
      auto weak_peer = peer_promise.get_future().get();

      // Hold dispatcher callbacks so the I/O thread can destroy the peer first.
      pqrs::dispatcher::extra::dispatcher_client blocker(dispatcher);
      std::promise<void> started;
      std::promise<void> release;
      auto release_future = release.get_future().share();
      blocker.enqueue_to_dispatcher([&] {
        started.set_value();
        release_future.wait();
      });
      started.get_future().wait();

      // Trigger either local close path, then resume the dispatcher only after
      // the peer has released its last shared owner.
      std::promise<asio::error_code> request_result;
      if (request_timeout) {
        server->async_request(id, {1}, std::chrono::milliseconds(100),
                              [&](const auto& error_code, auto) { request_result.set_value(error_code); });
      } else {
        server->async_close_peer(id);
      }
      expect(wait_until([&] { return weak_peer.expired(); }));
      release.set_value();
      blocker.detach_from_dispatcher();

      // The server must still deliver peer_closed and remove the exposed ID.
      expect(wait_until([&] { return closed_count.load() == 1; }));
      if (request_timeout) {
        expect(request_result.get_future().get() == asio::error::timed_out);
      }
      std::promise<bool> exposed;
      server->enqueue_to_dispatcher([&] {
        exposed.set_value(pqrs::unix_domain_stream::impl::server_test_access::has_exposed_peer(*server, id));
      });
      expect(!exposed.get_future().get());
      // Closing the same ID again and shutting down must not notify twice.
      server->async_close_peer(id);
      socket.close();
      server->async_shutdown();
      server.reset();
      expect(closed_count.load() == 1);
      dispatcher->terminate();
    }
  };

  "unix_domain_stream::server_peer_closed_follows_socket_close"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_peer_closed_follows_socket_close)" << std::endl;

    // Connect a raw socket so peer_closed can verify EOF at notification time.
    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);
    prepare_socket_file_path(server_socket_file_path);
    auto server = std::make_shared<pqrs::unix_domain_stream::impl::server_state>(
        dispatcher, server_socket_file_path, make_options().server,
        pqrs::unix_domain_stream::default_verify_peer);
    std::promise<void> bound;
    server->bound.connect([&] { bound.set_value(); });
    server->async_start();
    expect(bound.get_future().wait_for(std::chrono::seconds(3)) == std::future_status::ready);
    std::promise<pqrs::unix_domain_stream::peer_id> connected;
    server->peer_connected.connect([&](auto id, const auto&) { connected.set_value(id); });
    asio::io_context io_ctx;
    asio::local::stream_protocol::socket socket(io_ctx);
    socket.connect(asio::local::stream_protocol::endpoint(server_socket_file_path));
    socket.non_blocking(true);
    auto id = connected.get_future().get();
    std::atomic_size_t closed_count = 0;
    server->peer_closed.connect([&](auto closed_id) {
      expect(closed_id == id);
      expect(dispatcher->dispatcher_thread());
      std::array<uint8_t, 1024> buffer;
      asio::error_code error_code;
      // Drain any heartbeat frames; an open socket would return would_block.
      while (socket.read_some(asio::buffer(buffer), error_code) > 0) {
      }
      expect(error_code == asio::error::eof);
      ++closed_count;
    });

    // Pause the I/O thread after close_peer queues async_close, but before
    // async_close can run. The dispatcher must not report peer_closed yet.
    std::promise<void> close_queued;
    std::promise<void> release;
    auto release_future = release.get_future().share();
    asio::post(pqrs::unix_domain_stream::impl::runtime_test_access::get_io_context(), [&] {
      pqrs::unix_domain_stream::impl::server_test_access::close_peer(*server, id);
      close_queued.set_value();
      release_future.wait();
    });
    close_queued.get_future().wait();
    expect(wait_dispatcher_barrier(dispatcher));
    expect(closed_count.load() == 0);

    // Resume socket closure and check that exactly one notification follows.
    release.set_value();
    expect(wait_until([&] { return closed_count.load() == 1; }));
    server->async_shutdown();
    server.reset();
    socket.close();
    expect(closed_count.load() == 1);
    dispatcher->terminate();
  };

  "unix_domain_stream::server_async_request_close_peer"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_async_request_close_peer)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;
    std::atomic_size_t client_request_received_count = 0;
    std::atomic<pqrs::unix_domain_stream::peer_id> connected_peer_id = 0;

    // Start the server and remember the peer id assigned to the client.
    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto peer_id, auto&&) {
      connected_peer_id = peer_id;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    // Connect one client so the server has a target peer for async_request.
    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->request_received.connect([&](auto, auto&&) {
      ++client_request_received_count;
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));
    expect(wait_until([&] { return connected_peer_id.load() != 0_i; }));

    std::promise<async_request_test_result> promise;
    auto future = promise.get_future();

    // Send a server-side request and leave it pending.
    server->async_request(connected_peer_id,
                          std::vector<uint8_t>{1},
                          [&promise](const auto& error_code, auto response) {
                            promise.set_value({error_code, response});
                          });

    // Closing the peer locally after the request is pending should complete it.
    expect(wait_until([&] { return client_request_received_count.load() == 1_i; }));
    server->async_close_peer(connected_peer_id);

    // The callback should report the local close as operation_aborted.
    expect(future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto [error_code, response] = future.get();
    expect(error_code == asio::error::operation_aborted);
    expect(response == nullptr);
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_async_request_peer_closed"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_async_request_peer_closed)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;
    std::atomic_size_t client_request_received_count = 0;
    std::atomic<pqrs::unix_domain_stream::peer_id> connected_peer_id = 0;

    // Start the server and remember the peer id assigned to the client.
    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->peer_connected.connect([&](auto peer_id, auto&&) {
      connected_peer_id = peer_id;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    // Connect one client and count server-side requests that reach it.
    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connected.connect([&](auto&&) {
      client_connected = true;
    });
    client->request_received.connect([&](auto, auto&&) {
      ++client_request_received_count;
    });
    client->async_start();
    expect(wait_until([&] { return client_connected.load(); }));
    expect(wait_until([&] { return connected_peer_id.load() != 0_i; }));

    std::promise<async_request_test_result> promise;
    auto future = promise.get_future();

    // Send a server-side request and leave it pending on the client.
    server->async_request(connected_peer_id,
                          std::vector<uint8_t>{1},
                          [&promise](const auto& error_code, auto response) {
                            promise.set_value({error_code, response});
                          });

    // Close the remote peer after it has received the request but before it responds.
    expect(wait_until([&] { return client_request_received_count.load() == 1_i; }));
    client.reset();

    // The callback should report the remote close as connection_reset.
    expect(future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto [error_code, response] = future.get();
    expect(error_code == asio::error::connection_reset);
    expect(response == nullptr);
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_async_request_not_connected"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_async_request_not_connected)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();

    test_server server(dispatcher,
                       server_socket_file_path,
                       options);

    std::promise<async_request_test_result> promise;
    auto future = promise.get_future();

    server->async_request(1,
                          std::vector<uint8_t>{1},
                          [&promise](const auto& error_code, auto response) {
                            promise.set_value({error_code, response});
                          });

    expect(future.wait_for(std::chrono::milliseconds(3000)) == std::future_status::ready);

    auto [error_code, response] = future.get();
    expect(error_code == asio::error::not_connected);
    expect(response == nullptr);

    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::client_reconnect_interval"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::client_reconnect_interval)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = test_options(test_options::make_parameters(
        {},
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
        }));

    std::atomic_size_t connect_failed_count = 0;
    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connect_failed.connect([&](auto&& error_code) {
      expect(static_cast<bool>(error_code));
      ++connect_failed_count;
    });
    client->async_start();

    expect(wait_until([&] { return connect_failed_count.load() >= 3_i; },
                      std::chrono::milliseconds(1000)));

    // A missing server should retry at reconnect_interval pace, not spin in a
    // tight connect_failed loop.
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    expect(wait_dispatcher_barrier(dispatcher));
    expect(connect_failed_count.load() <= 8_i);

    client.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::client_reconnect_zero_interval"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::client_reconnect_zero_interval)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = test_options(test_options::make_parameters(
        {},
        {
            .reconnect_interval = std::chrono::milliseconds(0),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
        }));

    std::atomic_size_t connect_failed_count = 0;
    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connect_failed.connect([&](auto&& error_code) {
      expect(static_cast<bool>(error_code));
      ++connect_failed_count;
    });
    client->async_start();

    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    expect(wait_dispatcher_barrier(dispatcher));
    expect(connect_failed_count.load() > 0_i);
    expect(connect_failed_count.load() <= 5_i);

    client.reset();

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
    test_client client(dispatcher,
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
    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    expect(wait_until([&] { return connected_count.load() == 1_i; }));
    expect(connected_count.load() == 1_i);
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::client_invalidate_connection"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::client_invalidate_connection)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();

    std::atomic_bool server_bound = false;
    std::atomic<size_t> client_connected_count = 0;
    std::atomic<size_t> client_received_count = 0;

    // Echo data so the test can verify that the second connection is usable.
    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->received.connect([&](auto peer_id, auto&& buffer) {
      server->async_send(peer_id, *buffer);
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    test_client client(dispatcher,
                       server_socket_file_path,
                       options);
    client->connected.connect([&](auto&&) {
      ++client_connected_count;
    });
    client->received.connect([&](auto&& buffer) {
      client_received_count += buffer->size();
    });
    client->async_start();
    expect(wait_until([&] { return client_connected_count.load() == 1_i; }));

    // Drop the current peer and let the normal reconnect path establish a new one.
    client->async_invalidate_connection();

    expect(wait_until([&] { return client_connected_count.load() >= 2_i; }));

    client->async_send(std::vector<uint8_t>(8, 42));

    expect(wait_until([&] { return client_received_count.load() == 8_i; }));
    expect(client_connected_count.load() >= 2_i);
    expect(client_received_count.load() == 8_i);
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::client_invalidate_connection_before_server_start"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::client_invalidate_connection_before_server_start)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = make_options();

    std::atomic<size_t> connected_count = 0;
    std::atomic<size_t> connect_failed_count = 0;

    test_client client(dispatcher,
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

    // Invalidate while the client is in the reconnect loop before any server exists.
    // The stale connect attempt must not be reused after the server starts.
    client->async_invalidate_connection();
    client->async_invalidate_connection();

    std::atomic_bool server_bound = false;
    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    expect(wait_until([&] { return connected_count.load() == 1_i; }));

    // Give any duplicate reconnect timer a chance to run.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    expect(wait_dispatcher_barrier(dispatcher));
    expect(connected_count.load() == 1_i);
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::max_message_size"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::max_message_size)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = test_options(test_options::make_parameters(
        {
            .max_message_size = 8,
            .max_send_queue_size = 128,
            .write_timeout = std::chrono::milliseconds(1000),
        },
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
        }));
    std::atomic_bool server_bound = false;
    std::atomic_bool error_occurred = false;

    // Configure both peers with a small outgoing user data limit.
    test_server server(dispatcher,
                       server_socket_file_path,
                       options);
    server->bound.connect([&] {
      server_bound = true;
    });
    server->async_start();
    expect(wait_until([&] { return server_bound.load(); }));

    std::atomic_bool client_connected = false;
    test_client client(dispatcher,
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
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::server_socket_path_health_check"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::server_socket_path_health_check)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = test_options(test_options::make_parameters(
        {
            .max_send_queue_size = 128,
            .write_timeout = std::chrono::milliseconds(1000),
        },
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
            .socket_path_health_check_interval = std::chrono::milliseconds(100),
        }));

    std::atomic<size_t> bound_count = 0;
    std::atomic<size_t> closed_count = 0;
    std::atomic<size_t> peer_connected_count = 0;
    std::atomic<size_t> client_received_count = 0;
    std::atomic<size_t> verify_peer_count = 0;

    // Server health checks are internal and must not look like normal peers.
    test_server server(
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
    test_client client(dispatcher,
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
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::read_timeout"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::read_timeout)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = test_options(test_options::make_parameters(
        {
            .max_send_queue_size = 128,
            .read_timeout = std::chrono::milliseconds(200),
            .write_timeout = std::chrono::milliseconds(1000),
        },
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
        }));

    std::atomic_bool server_bound = false;
    std::atomic_bool peer_error_occurred = false;
    std::atomic_bool peer_closed = false;

    // Use a raw socket so only the frame header is sent.
    test_server server(dispatcher,
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
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::malformed_frame"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::malformed_frame)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = test_options(test_options::make_parameters(
        {
            .max_message_size = 8,
            .max_send_queue_size = 128,
            .read_timeout = std::chrono::milliseconds(1000),
            .write_timeout = std::chrono::milliseconds(1000),
        },
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
        }));

    std::atomic_bool server_bound = false;
    std::atomic_size_t peer_connected_count = 0;
    std::atomic_size_t peer_closed_count = 0;
    std::atomic_size_t message_size_error_count = 0;
    std::atomic_size_t invalid_argument_error_count = 0;

    test_server server(dispatcher,
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
        .frame = make_raw_frame(options.server.max_message_size +
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
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::heartbeat_timeout"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::heartbeat_timeout)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = test_options(test_options::make_parameters(
        {
            .max_send_queue_size = 128,
            .heartbeat_interval = std::chrono::milliseconds(1000),
            .heartbeat_timeout = std::chrono::milliseconds(300),
            .read_timeout = std::chrono::milliseconds(1000),
            .write_timeout = std::chrono::milliseconds(1000),
        },
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
        }));

    std::atomic_bool server_bound = false;
    std::atomic_bool peer_connected = false;
    std::atomic_bool peer_error_occurred = false;
    std::atomic_bool peer_closed = false;

    // Connect a raw socket that never sends any frames or heartbeats.
    test_server server(dispatcher,
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
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  "unix_domain_stream::heartbeat"_test = [] {
    std::cout << "TEST_CASE(unix_domain_stream::heartbeat)" << std::endl;

    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    prepare_socket_file_path(server_socket_file_path);

    auto options = test_options(test_options::make_parameters(
        {
            .max_send_queue_size = 128,
            .heartbeat_interval = std::chrono::milliseconds(50),
            .heartbeat_timeout = std::chrono::milliseconds(300),
            .write_timeout = std::chrono::milliseconds(1000),
        },
        {
            .reconnect_interval = std::chrono::milliseconds(100),
        },
        {
            .bind_retry_interval = std::chrono::milliseconds(100),
        }));

    std::atomic<size_t> server_received_count = 0;
    std::atomic<size_t> client_received_count = 0;
    std::atomic_bool server_bound = false;
    std::atomic_bool client_connected = false;

    // Echo user data while both sides are also exchanging heartbeat frames.
    test_server server(dispatcher,
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

    test_client client(dispatcher,
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
    client.reset();
    server.reset();

    dispatcher->terminate();
    dispatcher = nullptr;
  };

  return 0;
}
