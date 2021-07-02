use std::collections::HashMap;
use std::io::{BufReader, Cursor};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytestring::ByteString;
use mqttv5::LastWill;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::{rustls, TlsAcceptor};
use warp::{Filter, Reply};

use crate::client_loop::run as client_loop;
use crate::config::{Config, HttpConfig, TcpConfig};
use crate::message::Message;
use crate::metrics::InternalMetrics;
use crate::storage::Storage;

#[derive(Debug)]
pub enum Control {
    SessionTakenOver(oneshot::Sender<()>),
}

pub struct ServerState {
    pub config: Config,
    pub connections: RwLock<HashMap<ByteString, mpsc::UnboundedSender<Control>>>,
    pub storage: Box<dyn Storage>,
    pub session_timeouts: Mutex<HashMap<ByteString, JoinHandle<()>>>,
    pub metrics: Arc<InternalMetrics>,
    pub stat_sender: watch::Sender<HashMap<ByteString, ByteString>>,
    pub stat_receiver: watch::Receiver<HashMap<ByteString, ByteString>>,
}

impl ServerState {
    pub async fn new(config: Config, storage: Box<dyn Storage>) -> Result<Arc<Self>> {
        let (stat_sender, stat_receiver) = watch::channel(HashMap::new());
        let state = Arc::new(ServerState {
            config,
            connections: RwLock::new(HashMap::new()),
            storage,
            session_timeouts: Mutex::new(HashMap::new()),
            metrics: Arc::new(InternalMetrics::default()),
            stat_sender,
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
    state: Arc<ServerState>,
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

async fn run_tcp_server(state: Arc<ServerState>, tcp_config: TcpConfig) -> Result<()> {
    let port = tcp_config.port();

    tracing::info!(
        host = %tcp_config.host,
        port = port,
        "tcp listening",
    );

    if let Some(tls_config) = &tcp_config.tls {
        let cert_data = std::fs::read(&tls_config.cert)
            .with_context(|| format!("failed to read certificates file: {}", tls_config.cert))?;
        let key_data = std::fs::read(&tls_config.key)
            .with_context(|| format!("failed to read key file: {}", tls_config.cert))?;

        let cert = rustls::internal::pemfile::certs(&mut BufReader::new(Cursor::new(cert_data)))
            .map_err(|_| anyhow::anyhow!("failed to load tls certificates"))?;
        let mut keys =
            rustls::internal::pemfile::rsa_private_keys(&mut BufReader::new(Cursor::new(key_data)))
                .map_err(|_| anyhow::anyhow!("failed to load tls key"))?;
        let mut config = ServerConfig::new(rustls::NoClientAuth::new());
        config
            .set_single_cert(cert, keys.pop().unwrap())
            .context("failed to set tls certificate")?;
        let config = Arc::new(config);

        let listener = TcpListener::bind((tcp_config.host.as_str(), port)).await?;

        loop {
            let (stream, addr) = listener.accept().await?;
            let acceptor = TlsAcceptor::from(config.clone());
            if let Ok(stream) = acceptor.accept(stream).await {
                let state = state.clone();
                tokio::spawn(async move {
                    tracing::debug!(
                        protocol = "tcp",
                        remote_addr = %addr,
                        "incoming connection",
                    );

                    let (reader, writer) = tokio::io::split(stream);
                    client_loop(reader, writer, addr.to_string(), state).await;

                    tracing::debug!(
                        protocol = "tcp",
                        remote_addr = %addr,
                        "connection disconnected",
                    );
                });
            }
        }
    } else {
        let listener = TcpListener::bind((tcp_config.host.as_str(), port)).await?;

        loop {
            let (stream, addr) = listener.accept().await?;
            let state = state.clone();

            tokio::spawn(async move {
                tracing::debug!(
                    protocol = "tcp",
                    remote_addr = %addr,
                    "incoming connection",
                );

                let (reader, writer) = tokio::io::split(stream);
                client_loop(reader, writer, addr.to_string(), state).await;

                tracing::debug!(
                    protocol = "tcp",
                    remote_addr = %addr,
                    "connection disconnected",
                );
            });
        }
    }
}

async fn run_http_server(state: Arc<ServerState>, http_config: HttpConfig) -> Result<()> {
    let port = http_config.port();

    tracing::info!(
        host = %http_config.host,
        port = port,
        "http listening",
    );

    let mut routes = warp::path!("health").map(|| "OK".into_response()).boxed();

    if http_config.websocket {
        tracing::info!("websocket transport enabled");
        routes = routes
            .or(warp::path!("ws").and(crate::ws_transport::handler(state.clone())))
            .unify()
            .boxed();
    }

    if http_config.api {
        tracing::info!("api enabled");

        let api = warp::path!("api" / "v1" / ..)
            .and(crate::api::stat(state.clone()))
            .boxed();
        routes = routes.or(api).unify().boxed();
    }

    if let Some(tls_config) = &http_config.tls {
        warp::serve(routes)
            .tls()
            .cert_path(&tls_config.cert)
            .key_path(&tls_config.key)
            .bind((http_config.host.parse::<IpAddr>()?, port))
            .await;
    } else {
        warp::serve(routes)
            .run((http_config.host.parse::<IpAddr>()?, port))
            .await;
    }

    Ok(())
}

pub async fn run(state: Arc<ServerState>) -> Result<()> {
    let mut servers = Vec::new();

    if let Some(tcp_config) = state.config.network.tcp.clone() {
        let state = state.clone();
        servers.push(tokio::spawn(async move {
            if let Err(err) = run_tcp_server(state, tcp_config).await {
                tracing::error!(
                    error = %err,
                    "tcp server",
                );
            }
        }));
    }

    if let Some(http_config) = state.config.network.http.clone() {
        let state = state.clone();
        servers.push(tokio::spawn(async move {
            if let Err(err) = run_http_server(state, http_config).await {
                tracing::error!(
                    error = %err,
                    "tcp server",
                );
            }
        }));
    }

    for handle in servers {
        handle.await.ok();
    }
    Ok(())
}
