# example

## Expected result

```shell
make all
make run
```

```text
./build/example
server bound
client connected
peer_pid: 92476
peer_uid: 501
peer_gid: 20
server verify_peer
peer_pid: 92476
peer_uid: 501
peer_gid: 20
server peer_connected peer_id:1
peer_pid: 92476
peer_uid: 501
peer_gid: 20
server received peer_id:1 size:1
buffer: `1`
server received peer_id:1 size:2
buffer: `12`
client received size:1
buffer: `1`
client received size:2
buffer: `12`
server received peer_id:1 size:30720
buffer: `33333333333333333333333333333333333333333... (30720bytes)`
server received peer_id:1 size:23
buffer: `Type control-c to quit.`
server request_received peer_id:1 request_id:1 size:9
buffer: `request 1`
server request_received peer_id:1 request_id:2 size:9
buffer: `request 2`
client received size:30720
buffer: `33333333333333333333333333333333333333333... (30720bytes)`
client received size:23
buffer: `Type control-c to quit.`
client async_request 1 response size:22
buffer: `response for request 1`
client async_request 2 response size:22
buffer: `response for request 2`
```

Note: The order of the lines may change.
