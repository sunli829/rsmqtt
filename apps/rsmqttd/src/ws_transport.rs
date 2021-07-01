use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use bytes::Bytes;
use futures_util::{Sink, SinkExt, StreamExt, TryStreamExt};
use tokio::io::AsyncWrite;
use warp::reply::Response;
use warp::ws::{Message as WsMessage, Ws};
use warp::{Filter, Rejection, Reply};

use crate::client_loop::run as client_loop;
use crate::server::ServerState;

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

        self.0
            .start_send_unpin(WsMessage::binary(buf))
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err.to_string()))?;
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

pub fn handler(state: Arc<ServerState>) -> impl Filter<Extract = (Response,), Error = Rejection> {
    warp::path!("ws")
        .and(warp::get())
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

            warp::reply::with_header(reply, "Sec-WebSocket-Protocol", "mqtt").into_response()
        })
}
