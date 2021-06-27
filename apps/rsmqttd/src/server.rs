use std::collections::HashMap;
use std::io::{BufReader, Cursor, Error, ErrorKind};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use anyhow::{Context, Result};
use bytes::Bytes;
use bytestring::ByteString;
use futures_util::{Sink, SinkExt, StreamExt, TryStreamExt};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::{rustls, TlsAcceptor};
use warp::ws::{Message as WsMessage, Ws};
use warp::Filter;

use crate::client_loop::run as client_loop;
use crate::config::{Config, TcpConfig, WebsocketConfig};
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

struct SinkWriter<T>(T);

impl<T> AsyncWrite for SinkWriter<T>
where
    T: Sink<WsMessage, Error = warp::Error> + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        match self.0.poll_ready_unpin(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => {
                return Poll::Ready(Err(std::io::Error::new(ErrorKind::Other, err.to_string())))
            }
            Poll::Pending => return Poll::Pending,
        }

        let _ = self.0.start_send_unpin(WsMessage::binary(buf));
        self.0
            .poll_flush_unpin(cx)
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err.to_string()))
            .map_ok(|_| buf.len())
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Error>> {
        self.0
            .poll_flush_unpin(cx)
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err.to_string()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Error>> {
        self.0
            .poll_close_unpin(cx)
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err.to_string()))
    }
}

async fn run_websocket_server(
    state: Arc<ServerState>,
    websocket_config: WebsocketConfig,
) -> Result<()> {
    let port = websocket_config.port();

    tracing::info!(
        host = %websocket_config.host,
        port = port,
        "websocket listening",
    );

    let mqtt_ws = warp::get()
        .and(warp::filters::addr::remote())
        .and(warp::ws())
        .map(move |addr: Option<SocketAddr>, ws: Ws| {
            let state = state.clone();
            let reply = ws.on_upgrade(move |websocket| async move {
                let addr = addr
                    .map(|addr| addr.to_string())
                    .unwrap_or_else(|| "unknown".to_string());

                tracing::debug!(
                    protocol = "websocket",
                    remote_addr = %addr,
                    "incoming connection",
                );

                let (sink, stream) = websocket.split();

                let reader = tokio_util::io::StreamReader::new(
                    stream
                        .try_filter_map(|msg| async move {
                            if msg.is_binary() {
                                Ok(Some(Bytes::from(msg.into_bytes())))
                            } else {
                                Ok(None)
                            }
                        })
                        .map_err(|err| std::io::Error::new(ErrorKind::Other, err.to_string())),
                );
                tokio::pin!(reader);

                client_loop(reader, SinkWriter(sink), addr.to_string(), state).await;

                tracing::debug!(
                    protocol = "websocket",
                    remote_addr = %addr,
                    "connection disconnected",
                );
            });

            warp::reply::with_header(reply, "Sec-WebSocket-Protocol", "mqtt")
        });

    let ip_addr: IpAddr = websocket_config.host.parse()?;
    warp::serve(mqtt_ws).run((ip_addr, port)).await;
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

    if let Some(websocket_config) = state.config.network.websocket.clone() {
        let state = state.clone();
        servers.push(tokio::spawn(async move {
            if let Err(err) = run_websocket_server(state, websocket_config).await {
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
