# MQTT broker implemented in Rust

<div align="center">
  <!-- CI -->
  <img src="https://github.com/sunli829/rsmqtt/workflows/CI/badge.svg" />
  <!-- codecov -->
  <img src="https://codecov.io/gh/sunli829/rsmqtt/branch/master/graph/badge.svg" />
</div>

## TODO

- Server
    - [X] MQTT 5.0
    - [X] MQTT 3.1
    - [X] Publish
      - [X] Qos0
      - [X] Qos1
      - [X] Qos2
    - [X] Subscribe/Unsubscribe
    - [X] Last will
    - [X] Retain message
    - [X] Shared Subscriptions
    - [X] Websocket transport
    - [X] $SYS topics
    - [ ] Authentication exchange
    - [ ] Telemetry
- [ ] Admin UI
- Test
  - [ ] Framework
- API
  - [ ] Rest API
  - [ ] GraphQL API
- Storage
    - [X] Memory
    - [ ] Rocksdb
- [ ] ACL
- Rule engine
    - [ ] Lua
    - [ ] WASM
