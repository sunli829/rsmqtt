use std::io::{BufReader, Cursor};
use std::net::IpAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use service::{client_loop, RemoteAddr, ServiceState};
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::{rustls, TlsAcceptor};
use warp::{Filter, Reply};

use crate::config::{HttpConfig, NetworkConfig, TcpConfig};

async fn run_tcp_server(state: Arc<ServiceState>, tcp_config: TcpConfig) -> Result<()> {
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
                    client_loop(
                        state,
                        reader,
                        writer,
                        RemoteAddr {
                            protocol: "tcp",
                            addr: Some(addr.to_string()),
                        },
                    )
                    .await;

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
                client_loop(
                    state,
                    reader,
                    writer,
                    RemoteAddr {
                        protocol: "tcp",
                        addr: Some(addr.to_string()),
                    },
                )
                .await;

                tracing::debug!(
                    protocol = "tcp",
                    remote_addr = %addr,
                    "connection disconnected",
                );
            });
        }
    }
}

async fn run_http_server(state: Arc<ServiceState>, http_config: HttpConfig) -> Result<()> {
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

pub async fn run(state: Arc<ServiceState>, network_config: NetworkConfig) -> Result<()> {
    let mut servers = Vec::new();

    if let Some(tcp_config) = network_config.tcp {
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

    if let Some(http_config) = network_config.http {
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
