use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytestring::ByteString;
use codec::LastWill;
use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock};
use tokio::task::JoinHandle;

use crate::config::ServiceConfig;
use crate::message::Message;
use crate::metrics::InternalMetrics;
use crate::plugin::Plugin;
use crate::storage::Storage;

#[derive(Debug)]
pub enum Control {
    SessionTakenOver(oneshot::Sender<()>),
}

pub struct ServiceState {
    pub(crate) config: ServiceConfig,
    pub(crate) connections: RwLock<HashMap<ByteString, mpsc::UnboundedSender<Control>>>,
    pub(crate) storage: Box<dyn Storage>,
    pub(crate) session_timeouts: Mutex<HashMap<ByteString, JoinHandle<()>>>,
    pub(crate) metrics: Arc<InternalMetrics>,
    pub(crate) stat_sender: watch::Sender<HashMap<ByteString, ByteString>>,
    pub(crate) plugins: Vec<(&'static str, Box<dyn Plugin>)>,
    pub stat_receiver: watch::Receiver<HashMap<ByteString, ByteString>>,
}

impl ServiceState {
    pub async fn try_new(
        config: ServiceConfig,
        storage: Box<dyn Storage>,
        plugins: Vec<(&'static str, Box<dyn Plugin>)>,
    ) -> Result<Arc<Self>> {
        let (stat_sender, stat_receiver) = watch::channel(HashMap::new());
        let state = Arc::new(Self {
            config,
            connections: RwLock::new(HashMap::new()),
            storage,
            session_timeouts: Mutex::new(HashMap::new()),
            metrics: Arc::new(InternalMetrics::default()),
            stat_sender,
            plugins,
            stat_receiver,
        });

        let sessions = state.storage.get_sessions().await?;
        for session in sessions {
            add_session_timeout_handle(
                state.clone(),
                session.client_id,
                session.last_will,
                session.session_expiry_interval,
                session.last_will_expiry_interval,
            )
            .await;
        }

        Ok(state)
    }
}

pub async fn add_session_timeout_handle(
    state: Arc<ServiceState>,
    client_id: ByteString,
    last_will: Option<LastWill>,
    session_expiry_interval: u32,
    last_will_expiry_interval: u32,
) {
    let session_timeout_handle = {
        let state = state.clone();
        let client_id = client_id.clone();

        tokio::spawn(async move {
            let last_will_expiry_interval = last_will_expiry_interval.min(session_expiry_interval);
            let session_expiry_interval = if last_will_expiry_interval <= session_expiry_interval {
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

                if let Err(err) = state
                    .storage
                    .publish(vec![Message::from_last_will(last_will)])
                    .await
                {
                    tracing::error!(
                        error = %err,
                        "failed to publish last will message",
                    )
                }
            }

            tokio::time::sleep(Duration::from_secs(session_expiry_interval as u64)).await;

            tracing::debug!(
                client_id = %client_id,
                "session timeout",
            );

            if let Err(err) = state.storage.remove_session(&client_id).await {
                tracing::error!(
                    error = %err,
                    "failed to remove session",
                )
            }
            state.session_timeouts.lock().await.remove(&client_id);
            state.metrics.inc_clients_expired(1);
        })
    };
    state
        .session_timeouts
        .lock()
        .await
        .insert(client_id, session_timeout_handle);
}
