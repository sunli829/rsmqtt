use std::sync::atomic::Ordering;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::state::ServiceMetrics;
use crate::storage::StorageMetrics;

#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize)]
pub struct MetricsLoad {
    pub min1: f64,
    pub min5: f64,
    pub min15: f64,
}

#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize)]
pub struct Metrics {
    pub uptime: u64,
    pub bytes_received: usize,
    pub bytes_sent: usize,
    pub clients_connected: usize,
    pub clients_expired: usize,
    pub clients_disconnected: usize,
    pub clients_maximum: usize,
    pub clients_total: usize,
    pub messages_inflight: usize,
    pub messages_received: usize,
    pub messages_sent: usize,
    pub publish_messages_dropped: usize,
    pub publish_messages_received: usize,
    pub publish_messages_sent: usize,
    pub publish_bytes_received: usize,
    pub publish_bytes_sent: usize,
    pub retained_messages_count: usize,
    pub store_messages_count: usize,
    pub store_messages_bytes: usize,
    pub subscriptions_count: usize,
    pub load_messages_received: MetricsLoad,
    pub load_messages_sent: MetricsLoad,
    pub load_publish_dropped: MetricsLoad,
    pub load_publish_received: MetricsLoad,
    pub load_publish_sent: MetricsLoad,
    pub load_publish_bytes_received: MetricsLoad,
    pub load_publish_bytes_sent: MetricsLoad,
    pub load_bytes_received: MetricsLoad,
    pub load_bytes_sent: MetricsLoad,
    pub load_sockets: MetricsLoad,
    pub load_connections: MetricsLoad,
}

#[derive(Default)]
struct LoadCalc {
    duration: f64,
    initial: bool,
    prev_value: f64,
    value: f64,
}

impl LoadCalc {
    fn new(duration: f64) -> Self {
        Self {
            duration,
            initial: true,
            prev_value: 0.0,
            value: 0.0,
        }
    }

    fn update(&mut self, interval_seconds: u64, value: f64) -> &Self {
        let exponent = (-1.0 * interval_seconds as f64 / self.duration).exp();

        if self.initial {
            self.value = value;
            self.initial = false;
        } else {
            self.value = value + exponent * (self.value - value);
        }
        self
    }

    fn update_interval(&mut self, interval_seconds: u64, value: f64) -> &Self {
        let a = self.duration / interval_seconds as f64;
        self.update(interval_seconds, (value - self.prev_value) * a);
        self.prev_value = value;
        self
    }
}

pub struct MetricsCalc {
    max_clients: usize,
    start_time: Instant,
    last_update: u64,

    msgs_received_load1: LoadCalc,
    msgs_sent_load1: LoadCalc,
    msgs_dropped_load1: LoadCalc,
    pub_msgs_received_load1: LoadCalc,
    pub_msgs_sent_load1: LoadCalc,
    pub_bytes_received_load1: LoadCalc,
    pub_bytes_sent_load1: LoadCalc,
    bytes_received_load1: LoadCalc,
    bytes_sent_load1: LoadCalc,
    sockets_load1: LoadCalc,
    connections_load1: LoadCalc,

    msgs_received_load5: LoadCalc,
    msgs_sent_load5: LoadCalc,
    msgs_dropped_load5: LoadCalc,
    pub_msgs_received_load5: LoadCalc,
    pub_msgs_sent_load5: LoadCalc,
    pub_bytes_received_load5: LoadCalc,
    pub_bytes_sent_load5: LoadCalc,
    bytes_received_load5: LoadCalc,
    bytes_sent_load5: LoadCalc,
    sockets_load5: LoadCalc,
    connections_load5: LoadCalc,

    msgs_received_load15: LoadCalc,
    msgs_sent_load15: LoadCalc,
    msgs_dropped_load15: LoadCalc,
    pub_msgs_received_load15: LoadCalc,
    pub_msgs_sent_load15: LoadCalc,
    pub_bytes_received_load15: LoadCalc,
    pub_bytes_sent_load15: LoadCalc,
    bytes_received_load15: LoadCalc,
    bytes_sent_load15: LoadCalc,
    sockets_load15: LoadCalc,
    connections_load15: LoadCalc,
}

impl MetricsCalc {
    pub fn new() -> Self {
        Self {
            max_clients: 0,
            start_time: Instant::now(),
            last_update: 0,
            msgs_received_load1: LoadCalc::new(60.0),
            msgs_sent_load1: LoadCalc::new(60.0),
            msgs_dropped_load1: LoadCalc::new(60.0),
            pub_msgs_received_load1: LoadCalc::new(60.0),
            pub_msgs_sent_load1: LoadCalc::new(60.0),
            pub_bytes_received_load1: LoadCalc::new(60.0),
            pub_bytes_sent_load1: LoadCalc::new(60.0),
            bytes_received_load1: LoadCalc::new(60.0),
            bytes_sent_load1: LoadCalc::new(60.0),
            sockets_load1: LoadCalc::new(60.0),
            connections_load1: LoadCalc::new(60.0),
            msgs_received_load5: LoadCalc::new(300.0),
            msgs_sent_load5: LoadCalc::new(300.0),
            msgs_dropped_load5: LoadCalc::new(300.0),
            pub_msgs_received_load5: LoadCalc::new(300.0),
            pub_msgs_sent_load5: LoadCalc::new(300.0),
            pub_bytes_received_load5: LoadCalc::new(300.0),
            pub_bytes_sent_load5: LoadCalc::new(300.0),
            bytes_received_load5: LoadCalc::new(300.0),
            bytes_sent_load5: LoadCalc::new(300.0),
            sockets_load5: LoadCalc::new(300.0),
            connections_load5: LoadCalc::new(300.0),
            msgs_received_load15: LoadCalc::new(900.0),
            msgs_sent_load15: LoadCalc::new(900.0),
            msgs_dropped_load15: LoadCalc::new(900.0),
            pub_msgs_received_load15: LoadCalc::new(900.0),
            pub_msgs_sent_load15: LoadCalc::new(900.0),
            pub_bytes_received_load15: LoadCalc::new(900.0),
            pub_bytes_sent_load15: LoadCalc::new(900.0),
            bytes_received_load15: LoadCalc::new(900.0),
            bytes_sent_load15: LoadCalc::new(900.0),
            sockets_load15: LoadCalc::new(900.0),
            connections_load15: LoadCalc::new(900.0),
        }
    }

    pub fn update(
        &mut self,
        service_metrics: &ServiceMetrics,
        storage_metrics: &StorageMetrics,
    ) -> Metrics {
        let bytes_received = service_metrics.bytes_received.load(Ordering::Relaxed);
        let bytes_sent = service_metrics.bytes_sent.load(Ordering::Relaxed);
        let pub_bytes_received = service_metrics.pub_bytes_received.load(Ordering::Relaxed);
        let pub_bytes_sent = service_metrics.pub_bytes_sent.load(Ordering::Relaxed);
        let msgs_received = service_metrics.msgs_received.load(Ordering::Relaxed);
        let msgs_sent = service_metrics.msgs_sent.load(Ordering::Relaxed);
        let pub_msgs_received = service_metrics.pub_msgs_received.load(Ordering::Relaxed);
        let pub_msgs_sent = service_metrics.pub_msgs_sent.load(Ordering::Relaxed);
        let msgs_dropped = service_metrics.msgs_dropped.load(Ordering::Relaxed);
        let socket_connections = service_metrics.socket_connections.load(Ordering::Relaxed);
        let connection_count = service_metrics.connection_count.load(Ordering::Relaxed);
        let StorageMetrics {
            session_count,
            inflight_messages_count,
            retained_messages_count,
            messages_count,
            messages_bytes,
            subscriptions_count,
            clients_expired,
        } = *storage_metrics;

        self.max_clients = self.max_clients.max(connection_count);

        let uptime = (Instant::now() - self.start_time).as_secs();
        let interval_seconds = uptime - self.last_update;
        self.last_update = uptime;

        if interval_seconds > 0 {
            self.msgs_received_load1
                .update_interval(interval_seconds, msgs_received as f64);
            self.msgs_sent_load1
                .update_interval(interval_seconds, msgs_sent as f64);
            self.msgs_dropped_load1
                .update_interval(interval_seconds, msgs_dropped as f64);
            self.pub_msgs_received_load1
                .update_interval(interval_seconds, pub_msgs_received as f64);
            self.pub_msgs_sent_load1
                .update_interval(interval_seconds, pub_msgs_sent as f64);
            self.pub_bytes_received_load1
                .update_interval(interval_seconds, pub_bytes_received as f64);
            self.pub_bytes_sent_load1
                .update_interval(interval_seconds, pub_bytes_sent as f64);
            self.bytes_received_load1
                .update_interval(interval_seconds, bytes_received as f64);
            self.bytes_sent_load1
                .update_interval(interval_seconds, bytes_sent as f64);
            self.sockets_load1
                .update_interval(interval_seconds, socket_connections as f64);
            self.connections_load1
                .update_interval(interval_seconds, connection_count as f64);

            self.msgs_received_load5
                .update_interval(interval_seconds, msgs_received as f64);
            self.msgs_sent_load5
                .update_interval(interval_seconds, msgs_sent as f64);
            self.msgs_dropped_load5
                .update_interval(interval_seconds, msgs_dropped as f64);
            self.pub_msgs_received_load5
                .update_interval(interval_seconds, pub_msgs_received as f64);
            self.pub_msgs_sent_load5
                .update_interval(interval_seconds, pub_msgs_sent as f64);
            self.pub_bytes_received_load5
                .update_interval(interval_seconds, pub_bytes_received as f64);
            self.pub_bytes_sent_load5
                .update_interval(interval_seconds, pub_bytes_sent as f64);
            self.bytes_received_load5
                .update_interval(interval_seconds, bytes_received as f64);
            self.bytes_sent_load5
                .update_interval(interval_seconds, bytes_sent as f64);
            self.sockets_load5
                .update_interval(interval_seconds, socket_connections as f64);
            self.connections_load5
                .update_interval(interval_seconds, connection_count as f64);

            self.msgs_received_load15
                .update_interval(interval_seconds, msgs_received as f64);
            self.msgs_sent_load15
                .update_interval(interval_seconds, msgs_sent as f64);
            self.msgs_dropped_load15
                .update_interval(interval_seconds, msgs_dropped as f64);
            self.pub_msgs_received_load15
                .update_interval(interval_seconds, pub_msgs_received as f64);
            self.pub_msgs_sent_load15
                .update_interval(interval_seconds, pub_msgs_sent as f64);
            self.pub_bytes_received_load15
                .update_interval(interval_seconds, pub_bytes_received as f64);
            self.pub_bytes_sent_load15
                .update_interval(interval_seconds, pub_bytes_sent as f64);
            self.bytes_received_load15
                .update_interval(interval_seconds, bytes_received as f64);
            self.bytes_sent_load15
                .update_interval(interval_seconds, bytes_sent as f64);
            self.sockets_load15
                .update_interval(interval_seconds, socket_connections as f64);
            self.connections_load15
                .update_interval(interval_seconds, connection_count as f64);
        }

        Metrics {
            uptime,
            bytes_received,
            bytes_sent,
            clients_connected: connection_count,
            clients_expired,
            clients_disconnected: session_count - connection_count,
            clients_maximum: self.max_clients,
            clients_total: session_count,
            messages_inflight: inflight_messages_count,
            messages_received: msgs_received,
            messages_sent: msgs_sent,
            publish_messages_dropped: msgs_dropped,
            publish_messages_received: pub_msgs_received,
            publish_messages_sent: pub_msgs_sent,
            publish_bytes_received: pub_bytes_received,
            publish_bytes_sent: pub_bytes_sent,
            retained_messages_count,
            store_messages_count: messages_count,
            store_messages_bytes: messages_bytes,
            subscriptions_count,
            load_messages_received: MetricsLoad {
                min1: self.msgs_received_load1.value,
                min5: self.msgs_received_load5.value,
                min15: self.msgs_received_load15.value,
            },
            load_messages_sent: MetricsLoad {
                min1: self.msgs_sent_load1.value,
                min5: self.msgs_sent_load5.value,
                min15: self.msgs_sent_load15.value,
            },
            load_publish_dropped: MetricsLoad {
                min1: self.msgs_dropped_load1.value,
                min5: self.msgs_dropped_load5.value,
                min15: self.msgs_dropped_load15.value,
            },
            load_publish_received: MetricsLoad {
                min1: self.pub_msgs_received_load1.value,
                min5: self.pub_msgs_received_load5.value,
                min15: self.pub_msgs_received_load15.value,
            },
            load_publish_sent: MetricsLoad {
                min1: self.pub_msgs_sent_load1.value,
                min5: self.pub_msgs_sent_load5.value,
                min15: self.pub_msgs_sent_load15.value,
            },
            load_publish_bytes_received: MetricsLoad {
                min1: self.pub_bytes_received_load1.value,
                min5: self.pub_bytes_received_load5.value,
                min15: self.pub_bytes_received_load15.value,
            },
            load_publish_bytes_sent: MetricsLoad {
                min1: self.pub_bytes_sent_load1.value,
                min5: self.pub_bytes_sent_load5.value,
                min15: self.pub_bytes_sent_load15.value,
            },
            load_bytes_received: MetricsLoad {
                min1: self.bytes_received_load1.value,
                min5: self.bytes_received_load5.value,
                min15: self.bytes_received_load15.value,
            },
            load_bytes_sent: MetricsLoad {
                min1: self.bytes_sent_load1.value,
                min5: self.bytes_sent_load5.value,
                min15: self.bytes_sent_load15.value,
            },
            load_sockets: MetricsLoad {
                min1: self.sockets_load1.value,
                min5: self.sockets_load5.value,
                min15: self.sockets_load15.value,
            },
            load_connections: MetricsLoad {
                min1: self.connections_load1.value,
                min5: self.connections_load5.value,
                min15: self.connections_load15.value,
            },
        }
    }
}
