[![Build Status](https://github.com/pqrs-org/cpp-unix_domain_stream/workflows/CI/badge.svg)](https://github.com/pqrs-org/cpp-unix_domain_stream/actions)
[![License](https://img.shields.io/badge/license-Boost%20Software%20License-blue.svg)](https://github.com/pqrs-org/cpp-unix_domain_stream/blob/main/LICENSE.md)

# cpp-unix_domain_stream

Unix domain stream socket server and client.

- Server and client work asynchronously. (using `pqrs::dispatcher`)
- Server and client can be used safely in a multi-threaded environment.
- Communication is full-duplex over a single `SOCK_STREAM` connection.
- Messages are framed internally, so users can send and receive `std::vector<uint8_t>` messages without handling stream boundaries.
- Server manages each connected client by `pqrs::unix_domain_stream::peer_id`.
- Server can verify peer credentials before accepting a connection.
- Server will rebind automatically when bind or accept fails if `options::reconnect_interval` is set.
- Client will reconnect automatically when connect fails or the connection is closed if `options::reconnect_interval` is set.

## Requirements

cpp-unix_domain_stream depends on the following libraries.

- [asio](https://github.com/chriskohlhoff/asio/)
- [Nod](https://github.com/fr00b0/nod)
- [pqrs::dispatcher](https://github.com/pqrs-org/cpp-dispatcher)
- [pqrs::gsl](https://github.com/pqrs-org/cpp-gsl)

## Install

Copy `include/pqrs` and `vendor/vendor/include` directories into your include directory.
