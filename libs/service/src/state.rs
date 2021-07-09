use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytestring::ByteString;
use codec::LastWill;
use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock};
use tokio::task::JoinHandle;

use crate::config::ServiceConfig;
use crate::message::Message;
use crate::metrics::{Metrics, MetricsCalc};
use crate::plugin::Plugin;
use crate::rewrite::Rewrite;
use crate::storage::Storage;
use tokio_stream::Stream;

#[derive(Debug, Default)]
pub struct ServiceMetrics {
    pub bytes_received: AtomicUsize,
    pub bytes_sent: AtomicUsize,
    pub pub_bytes_received: AtomicUsize,
    pub pub_bytes_sent: AtomicUsize,
    pub msgs_received: AtomicUsize,
    pub msgs_sent: AtomicUsize,
    pub pub_msgs_received: AtomicUsize,
    pub pub_msgs_sent: AtomicUsize,
    pub msgs_dropped: AtomicUsize,
    pub clients_expired: AtomicUsize,
    pub socket_connections: AtomicUsize,
    pub connection_count: AtomicUsize,
}

impl ServiceMetrics {
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
}

#[derive(Debug)]
pub enum Control {
    SessionTakenOver(oneshot::Sender<()>),
}

pub struct ServiceState {
    pub config: ServiceConfig,
    pub(crate) connections: RwLock<HashMap<String, mpsc::UnboundedSender<Control>>>,
    pub(crate) storage: Storage,
    pub(crate) session_timeouts: Mutex<HashMap<String, JoinHandle<()>>>,
    pub(crate) service_metrics: Arc<ServiceMetrics>,
    pub(crate) plugins: Vec<(&'static str, Arc<dyn Plugin>)>,
    rewrites: Vec<Rewrite>,
    metrics_calc: Mutex<MetricsCalc>,
    metrics_sender: watch::Sender<Metrics>,
    metrics_receiver: watch::Receiver<Metrics>,
}

impl ServiceState {
    pub fn new(
        config: ServiceConfig,
        plugins: Vec<(&'static str, Arc<dyn Plugin>)>,
    ) -> Result<Arc<Self>> {
        let (stat_sender, stat_receiver) = watch::channel(Metrics::default());
        let mut rewrites = Vec::new();

        for rewrite_cfg in &config.rewrites {
            rewrites
                .push(Rewrite::try_new(rewrite_cfg).with_context(|| {
                    format!("invalid rewrite pattern: {}", rewrite_cfg.pattern)
                })?);
        }

        Ok(Arc::new(Self {
            config,
            connections: RwLock::new(HashMap::new()),
            storage: Storage::default(),
            session_timeouts: Mutex::new(HashMap::new()),
            service_metrics: Arc::new(ServiceMetrics::default()),
            metrics_sender: stat_sender,
            plugins,
            rewrites,
            metrics_receiver: stat_receiver,
            metrics_calc: Mutex::new(MetricsCalc::new()),
        }))
    }

    pub(crate) fn rewrite(&self, topic: &mut ByteString) {
        for rewrite in &self.rewrites {
            if let Some(new_topic) = rewrite.rewrite(topic) {
                *topic = new_topic.into();
                break;
            }
        }
    }

    pub async fn add_session_timeout_handle(
        state: Arc<ServiceState>,
        client_id: String,
        last_will: Option<LastWill>,
        session_expiry_interval: u32,
        last_will_expiry_interval: u32,
    ) {
        let session_timeout_handle = {
            let state = state.clone();
            let client_id = client_id.clone();

            tokio::spawn(async move {
                let last_will_expiry_interval =
                    last_will_expiry_interval.min(session_expiry_interval);
                let session_expiry_interval =
                    if last_will_expiry_interval <= session_expiry_interval {
                        session_expiry_interval - last_will_expiry_interval
                    } else {
                        0
                    };

                tokio::time::sleep(Duration::from_secs(last_will_expiry_interval as u64)).await;
                if let Some(last_will) = last_will {
                    tracing::debug!(
                        publisher = %client_id,
                        topic = %last_will.topic,
                        "send last will message",
                    );

                    state
                        .storage
                        .publish(std::iter::once(Message::from_last_will(last_will)));
                }

                tokio::time::sleep(Duration::from_secs(session_expiry_interval as u64)).await;

                tracing::debug!(
                    client_id = %client_id,
                    "session timeout",
                );

                state.storage.remove_session(&client_id);
                state.session_timeouts.lock().await.remove(&client_id);
                state.service_metrics.inc_clients_expired(1);
            })
        };
        state
            .session_timeouts
            .lock()
            .await
            .insert(client_id, session_timeout_handle);
    }

    pub async fn update_metrics(&self) {
        let metrics = self
            .metrics_calc
            .lock()
            .await
            .update(&self.service_metrics, &self.storage.metrics());
        self.metrics_sender.send(metrics).ok();
    }

    pub fn metrics(&self) -> Metrics {
        *self.metrics_receiver.borrow()
    }

    pub fn metrics_stream(&self) -> impl Stream<Item = Metrics> + Send + 'static {
        tokio_stream::wrappers::WatchStream::new(self.metrics_receiver.clone())
    }
}
