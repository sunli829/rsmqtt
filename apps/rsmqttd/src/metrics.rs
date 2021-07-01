use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub struct InternalMetrics {
    bytes_received: AtomicUsize,
    bytes_sent: AtomicUsize,
    pub_bytes_received: AtomicUsize,
    pub_bytes_sent: AtomicUsize,
    msgs_received: AtomicUsize,
    msgs_sent: AtomicUsize,
    pub_msgs_received: AtomicUsize,
    pub_msgs_sent: AtomicUsize,
    msgs_dropped: AtomicUsize,
    clients_expired: AtomicUsize,
    socket_connections: AtomicUsize,
    connection_count: AtomicUsize,
}

#[derive(Debug, Default)]
pub struct Metrics {
    pub bytes_received: usize,
    pub bytes_sent: usize,
    pub pub_bytes_received: usize,
    pub pub_bytes_sent: usize,
    pub msgs_received: usize,
    pub msgs_sent: usize,
    pub pub_msgs_received: usize,
    pub pub_msgs_sent: usize,
    pub msgs_dropped: usize,
    pub clients_expired: usize,
    pub socket_connections: usize,
    pub connection_count: usize,
}

impl InternalMetrics {
    #[inline]
    pub fn inc_bytes_received(&self, value: usize) {
        self.bytes_received.fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_bytes_sent(&self, value: usize) {
        self.bytes_sent.fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_pub_bytes_received(&self, value: usize) {
        self.pub_bytes_received.fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_pub_bytes_sent(&self, value: usize) {
        self.pub_bytes_sent.fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_msgs_received(&self, value: usize) {
        self.msgs_received.fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_msgs_sent(&self, value: usize) {
        self.msgs_sent.fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_pub_msgs_received(&self, value: usize) {
        self.pub_msgs_received.fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_pub_msgs_sent(&self, value: usize) {
        self.pub_msgs_sent.fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_msg_dropped(&self, value: usize) {
        self.msgs_dropped.fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_clients_expired(&self, value: usize) {
        self.clients_expired.fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_socket_connections(&self, value: usize) {
        self.clients_expired.fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn dec_socket_connections(&self, value: usize) {
        self.clients_expired.fetch_sub(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_connection_count(&self, value: usize) {
        self.connection_count.fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn dec_connection_count(&self, value: usize) {
        self.connection_count.fetch_sub(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(&self) -> Metrics {
        Metrics {
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            pub_bytes_received: self.pub_bytes_received.load(Ordering::Relaxed),
            pub_bytes_sent: self.pub_bytes_sent.load(Ordering::Relaxed),
            msgs_received: self.msgs_received.load(Ordering::Relaxed),
            msgs_sent: self.msgs_sent.load(Ordering::Relaxed),
            pub_msgs_received: self.pub_msgs_received.load(Ordering::Relaxed),
            pub_msgs_sent: self.pub_msgs_sent.load(Ordering::Relaxed),
            msgs_dropped: self.msgs_dropped.load(Ordering::Relaxed),
            clients_expired: self.clients_expired.load(Ordering::Relaxed),
            socket_connections: self.socket_connections.load(Ordering::Relaxed),
            connection_count: self.connection_count.load(Ordering::Relaxed),
        }
    }
}
