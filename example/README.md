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
peer_pid: ...
peer_uid: ...
peer_gid: ...
server peer_connected peer_id:1
peer_pid: ...
peer_uid: ...
peer_gid: ...
server received peer_id:1 size:1
buffer: `1`
server received peer_id:1 size:2
buffer: `12`
server received peer_id:1 size:30720
buffer: `33333333333333333333333333333333333333333... (30720bytes)`
server received peer_id:1 size:23
buffer: `Type control-c to quit.`
client received size:1
buffer: `1`
client received size:2
buffer: `12`
client received size:30720
buffer: `33333333333333333333333333333333333333333... (30720bytes)`
client received size:23
buffer: `Type control-c to quit.`
```

Note: The order of the lines may change.
