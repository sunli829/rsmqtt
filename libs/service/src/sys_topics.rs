use codec::Qos;

use crate::message::Message;
use crate::ServiceState;

impl ServiceState {
    pub fn update_sys_topics(&self) {
        let metrics = self.metrics();

        macro_rules! update {
            ($state:expr, $topic:literal, $payload:expr) => {
                $state.storage.deliver(std::iter::once(
                    Message::new(
                        $topic,
                        Qos::AtMostOnce,
                        bytes::Bytes::from($payload.to_string().into_bytes()),
                    )
                    .with_retain(true),
                ));
            };
        }

        update!(
            self,
            "$SYS/broker/uptime",
            format!("{} seconds", metrics.uptime)
        );

        update!(self, "$SYS/broker/bytes/received", metrics.bytes_received);
        update!(self, "$SYS/broker/bytes/sent", metrics.bytes_sent);

        update!(
            self,
            "$SYS/broker/clients/connected",
            metrics.clients_connected
        );
        update!(self, "$SYS/broker/clients/expired", metrics.clients_expired);
        update!(
            self,
            "$SYS/broker/clients/disconnected",
            metrics.clients_disconnected
        );
        update!(
            self,
            "$SYS/broker/clients/disconnected",
            metrics.clients_disconnected
        );
        update!(self, "$SYS/broker/clients/maximum", metrics.clients_maximum);
        update!(self, "$SYS/broker/clients/total", metrics.clients_total);

        update!(
            self,
            "$SYS/broker/messages/inflight",
            metrics.messages_inflight
        );
        update!(
            self,
            "$SYS/broker/messages/received",
            metrics.messages_received
        );
        update!(self, "$SYS/broker/messages/sent", metrics.messages_sent);
        update!(
            self,
            "$SYS/broker/publish/messages/dropped",
            metrics.publish_messages_dropped
        );
        update!(
            self,
            "$SYS/broker/publish/messages/received",
            metrics.publish_messages_received
        );
        update!(
            self,
            "$SYS/broker/publish/messages/sent",
            metrics.publish_messages_sent
        );
        update!(
            self,
            "$SYS/broker/publish/bytes/received",
            metrics.publish_bytes_received
        );
        update!(
            self,
            "$SYS/broker/publish/bytes/sent",
            metrics.publish_bytes_sent
        );

        update!(
            self,
            "$SYS/broker/retained messages/count",
            metrics.retained_messages_count
        );
        update!(
            self,
            "$SYS/broker/store/messages/count",
            metrics.store_messages_count
        );
        update!(
            self,
            "$SYS/broker/store/messages/bytes",
            metrics.store_messages_bytes
        );
        update!(
            self,
            "$SYS/broker/subscriptions/count",
            metrics.subscriptions_count
        );

        // 1min
        update!(
            self,
            "$SYS/broker/load/messages/received/1min",
            metrics.load_messages_received.min1
        );
        update!(
            self,
            "$SYS/broker/load/messages/sent/1min",
            metrics.load_messages_sent.min1
        );
        update!(
            self,
            "$SYS/broker/load/publish/dropped/1min",
            metrics.load_publish_dropped.min1
        );
        update!(
            self,
            "$SYS/broker/load/publish/received/1min",
            metrics.load_publish_received.min1
        );
        update!(
            self,
            "$SYS/broker/load/publish/sent/1min",
            metrics.load_publish_sent.min1
        );
        update!(
            self,
            "$SYS/broker/load/bytes/received/1min",
            metrics.load_bytes_received.min1
        );
        update!(
            self,
            "$SYS/broker/load/bytes/sent/1min",
            metrics.load_bytes_sent.min1
        );
        update!(
            self,
            "$SYS/broker/load/sockets/1min",
            metrics.load_sockets.min1
        );
        update!(
            self,
            "$SYS/broker/load/connections/1min",
            metrics.load_connections.min1
        );

        // 5min
        update!(
            self,
            "$SYS/broker/load/messages/received/5min",
            metrics.load_messages_received.min5
        );
        update!(
            self,
            "$SYS/broker/load/messages/sent/5min",
            metrics.load_messages_sent.min5
        );
        update!(
            self,
            "$SYS/broker/load/publish/dropped/5min",
            metrics.load_publish_dropped.min5
        );
        update!(
            self,
            "$SYS/broker/load/publish/received/5min",
            metrics.load_publish_received.min5
        );
        update!(
            self,
            "$SYS/broker/load/publish/sent/5min",
            metrics.load_publish_sent.min5
        );
        update!(
            self,
            "$SYS/broker/load/bytes/received/5min",
            metrics.load_bytes_received.min5
        );
        update!(
            self,
            "$SYS/broker/load/bytes/sent/5min",
            metrics.load_bytes_sent.min5
        );
        update!(
            self,
            "$SYS/broker/load/sockets/5min",
            metrics.load_sockets.min5
        );
        update!(
            self,
            "$SYS/broker/load/connections/5min",
            metrics.load_connections.min5
        );

        // 15min
        update!(
            self,
            "$SYS/broker/load/messages/received/15min",
            metrics.load_messages_received.min15
        );
        update!(
            self,
            "$SYS/broker/load/messages/sent/15min",
            metrics.load_messages_sent.min15
        );
        update!(
            self,
            "$SYS/broker/load/publish/dropped/15min",
            metrics.load_publish_dropped.min15
        );
        update!(
            self,
            "$SYS/broker/load/publish/received/15min",
            metrics.load_publish_received.min15
        );
        update!(
            self,
            "$SYS/broker/load/publish/sent/15min",
            metrics.load_publish_sent.min15
        );
        update!(
            self,
            "$SYS/broker/load/bytes/received/15min",
            metrics.load_bytes_received.min15
        );
        update!(
            self,
            "$SYS/broker/load/bytes/sent/15min",
            metrics.load_bytes_sent.min15
        );
        update!(
            self,
            "$SYS/broker/load/sockets/15min",
            metrics.load_sockets.min15
        );
        update!(
            self,
            "$SYS/broker/load/connections/15min",
            metrics.load_connections.min15
        );
    }
}
