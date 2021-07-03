use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytestring::ByteString;
use codec::Qos;
use version::version;

use crate::message::Message;
use crate::state::ServiceState;

struct Load {
    duration: f64,
    initial: bool,
    prev_value: f64,
    value: f64,
}

impl Load {
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

impl Display for Load {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:.2}", self.value)
    }
}

#[derive(Default)]
struct LastState {
    last_values: HashMap<ByteString, ByteString>,
    new_values: HashMap<ByteString, ByteString>,
}

impl LastState {
    fn update(&mut self, topic: impl Into<ByteString>, value: impl ToString) {
        let topic = topic.into();
        let value: ByteString = value.to_string().into();
        if self.last_values.get(&topic) == Some(&value) {
            return;
        }
        self.new_values.insert(topic, value);
    }

    fn merge(&mut self) {
        for (topic, value) in self.new_values.drain() {
            self.last_values.insert(topic, value);
        }
    }
}

pub async fn sys_topics_update_loop(state: Arc<ServiceState>) {
    if state.config.sys_update_interval == 0 {
        return;
    }

    let mut max_clients = 0;
    let start_time = Instant::now();
    let mut last_update = 0;
    let mut last_state = LastState::default();

    let mut msgs_received_load1 = Load::new(60.0);
    let mut msgs_sent_load1 = Load::new(60.0);
    let mut msgs_dropped_load1 = Load::new(60.0);
    let mut pub_msgs_received_load1 = Load::new(60.0);
    let mut pub_msgs_sent_load1 = Load::new(60.0);
    let mut bytes_received_load1 = Load::new(60.0);
    let mut bytes_sent_load1 = Load::new(60.0);
    let mut sockets_load1 = Load::new(60.0);
    let mut connections_load1 = Load::new(60.0);

    let mut msgs_received_load5 = Load::new(300.0);
    let mut msgs_sent_load5 = Load::new(300.0);
    let mut msgs_dropped_load5 = Load::new(300.0);
    let mut pub_msgs_received_load5 = Load::new(300.0);
    let mut pub_msgs_sent_load5 = Load::new(300.0);
    let mut bytes_received_load5 = Load::new(300.0);
    let mut bytes_sent_load5 = Load::new(300.0);
    let mut sockets_load5 = Load::new(300.0);
    let mut connections_load5 = Load::new(300.0);

    let mut msgs_received_load15 = Load::new(900.0);
    let mut msgs_sent_load15 = Load::new(900.0);
    let mut msgs_dropped_load15 = Load::new(900.0);
    let mut pub_msgs_received_load15 = Load::new(900.0);
    let mut pub_msgs_sent_load15 = Load::new(900.0);
    let mut bytes_received_load15 = Load::new(900.0);
    let mut bytes_sent_load15 = Load::new(900.0);
    let mut sockets_load15 = Load::new(900.0);
    let mut connections_load15 = Load::new(900.0);

    loop {
        tokio::time::sleep(Duration::from_secs(state.config.sys_update_interval)).await;

        let metrics = state.metrics.get();
        let storage_metrics = match state.storage.metrics().await {
            Ok(storage_metrics) => storage_metrics,
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "failed to load storage metrics",
                );
                continue;
            }
        };

        max_clients = max_clients.max(metrics.connection_count);

        let uptime = (Instant::now() - start_time).as_secs();

        last_state.update("broker/uptime", format!("{} seconds", uptime));

        last_state.update("broker/bytes/received", metrics.bytes_received);
        last_state.update("broker/bytes/sent", metrics.bytes_sent);

        last_state.update("broker/clients/connected", metrics.connection_count);
        last_state.update("broker/clients/expired", metrics.clients_expired);
        last_state.update(
            "broker/clients/disconnected",
            storage_metrics.session_count - metrics.connection_count,
        );
        last_state.update("broker/clients/maximum", max_clients);
        last_state.update("broker/clients/total", storage_metrics.session_count);

        last_state.update(
            "broker/messages/inflight",
            storage_metrics.inflight_messages_count,
        );
        last_state.update("broker/messages/received", metrics.msgs_received);
        last_state.update("broker/messages/sent", metrics.msgs_sent);
        last_state.update("broker/publish/messages/dropped", metrics.msgs_dropped);
        last_state.update(
            "broker/publish/messages/received",
            metrics.pub_msgs_received,
        );
        last_state.update("broker/publish/messages/sent", metrics.pub_msgs_sent);

        last_state.update(
            "broker/retained messages/count",
            storage_metrics.retained_messages_count,
        );
        last_state.update(
            "broker/store/messages/count",
            storage_metrics.messages_count,
        );
        last_state.update(
            "broker/store/messages/bytes",
            storage_metrics.messages_bytes,
        );

        last_state.update(
            "broker/subscriptions/count",
            storage_metrics.subscriptions_count,
        );
        last_state.update("broker/version", version!());

        let interval_seconds = uptime - last_update;
        if interval_seconds > 0 {
            // 1min
            last_state.update(
                "broker/load/messages/received/1min",
                &msgs_received_load1
                    .update_interval(interval_seconds, metrics.msgs_received as f64),
            );
            last_state.update(
                "broker/load/messages/sent/1min",
                &msgs_sent_load1.update_interval(interval_seconds, metrics.msgs_sent as f64),
            );
            last_state.update(
                "broker/load/publish/dropped/1min",
                &msgs_dropped_load1.update_interval(interval_seconds, metrics.msgs_dropped as f64),
            );
            last_state.update(
                "broker/load/publish/received/1min",
                &pub_msgs_received_load1
                    .update_interval(interval_seconds, metrics.pub_msgs_received as f64),
            );
            last_state.update(
                "broker/load/publish/sent/1min",
                &pub_msgs_sent_load1
                    .update_interval(interval_seconds, metrics.pub_msgs_sent as f64),
            );
            last_state.update(
                "broker/load/bytes/received/1min",
                &bytes_received_load1
                    .update_interval(interval_seconds, metrics.bytes_received as f64),
            );
            last_state.update(
                "broker/load/bytes/sent/1min",
                &bytes_sent_load1.update_interval(interval_seconds, metrics.bytes_sent as f64),
            );
            last_state.update(
                "broker/load/sockets/1min",
                &sockets_load1.update_interval(interval_seconds, metrics.socket_connections as f64),
            );
            last_state.update(
                "broker/load/connections/1min",
                &connections_load1
                    .update_interval(interval_seconds, metrics.connection_count as f64),
            );

            // 5min
            last_state.update(
                "broker/load/messages/received/5min",
                &msgs_received_load5
                    .update_interval(interval_seconds, metrics.msgs_received as f64),
            );
            last_state.update(
                "broker/load/messages/sent/5min",
                &msgs_sent_load5.update_interval(interval_seconds, metrics.msgs_sent as f64),
            );
            last_state.update(
                "broker/load/publish/dropped/5min",
                &msgs_dropped_load5.update_interval(interval_seconds, metrics.msgs_dropped as f64),
            );
            last_state.update(
                "broker/load/publish/received/5min",
                &pub_msgs_received_load5
                    .update_interval(interval_seconds, metrics.pub_msgs_received as f64),
            );
            last_state.update(
                "broker/load/publish/sent/5min",
                &pub_msgs_sent_load5
                    .update_interval(interval_seconds, metrics.pub_msgs_sent as f64),
            );
            last_state.update(
                "broker/load/bytes/received/5min",
                &bytes_received_load5
                    .update_interval(interval_seconds, metrics.bytes_received as f64),
            );
            last_state.update(
                "broker/load/bytes/sent/5min",
                &bytes_sent_load5.update_interval(interval_seconds, metrics.bytes_sent as f64),
            );
            last_state.update(
                "broker/load/sockets/5min",
                &sockets_load5.update_interval(interval_seconds, metrics.socket_connections as f64),
            );
            last_state.update(
                "broker/load/connections/5min",
                &connections_load5
                    .update_interval(interval_seconds, metrics.connection_count as f64),
            );

            // 15min
            last_state.update(
                "broker/load/messages/received/15min",
                &msgs_received_load15
                    .update_interval(interval_seconds, metrics.msgs_received as f64),
            );
            last_state.update(
                "broker/load/messages/sent/15min",
                &msgs_sent_load15.update_interval(interval_seconds, metrics.msgs_sent as f64),
            );
            last_state.update(
                "broker/load/publish/dropped/15min",
                &msgs_dropped_load15.update_interval(interval_seconds, metrics.msgs_dropped as f64),
            );
            last_state.update(
                "broker/load/publish/received/15min",
                &pub_msgs_received_load15
                    .update_interval(interval_seconds, metrics.pub_msgs_received as f64),
            );
            last_state.update(
                "broker/load/publish/sent/15min",
                &pub_msgs_sent_load15
                    .update_interval(interval_seconds, metrics.pub_msgs_sent as f64),
            );
            last_state.update(
                "broker/load/bytes/received/15min",
                &bytes_received_load15
                    .update_interval(interval_seconds, metrics.bytes_received as f64),
            );
            last_state.update(
                "broker/load/bytes/sent/15min",
                &bytes_sent_load15.update_interval(interval_seconds, metrics.bytes_sent as f64),
            );
            last_state.update(
                "broker/load/sockets/15min",
                &sockets_load15
                    .update_interval(interval_seconds, metrics.socket_connections as f64),
            );
            last_state.update(
                "broker/load/connections/15min",
                &connections_load15
                    .update_interval(interval_seconds, metrics.connection_count as f64),
            );
        }

        // publish to queue
        let mut msgs = Vec::new();
        for (topic, payload) in &last_state.new_values {
            msgs.push(
                Message::new(
                    format!("$SYS/{}", topic).into(),
                    Qos::AtMostOnce,
                    payload.clone().into_bytes(),
                )
                .with_retain(true),
            );
        }
        state.storage.publish(msgs).await.ok();
        last_state.merge();

        // publish to watch
        state.stat_sender.send(last_state.last_values.clone()).ok();

        last_update = uptime;
    }
}
