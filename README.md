# MQTT broker implemented in Rust

<div align="center">
  <!-- CI -->
  <img src="https://github.com/sunli829/rsmqtt/workflows/CI/badge.svg" />
  <!-- codecov -->
  <img src="https://codecov.io/gh/sunli829/rsmqtt/branch/master/graph/badge.svg" />
  <!-- Crates version -->
  <a href="https://crates.io/crates/rsmqttd">
    <img src="https://img.shields.io/crates/v/rsmqttd.svg?style=flat-square"
    alt="Crates.io version" />
  </a>
</div>

## Features

- MQTT 5.0/3.1
- Qos0/Qos1/Qos2
- Last Will
- Retained Messages
- Shared Subscriptions
- Tcp/WebSocket transport
- Authentication
- ACL([oso](https://crates.io/crates/oso))
