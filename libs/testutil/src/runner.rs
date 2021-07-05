use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytestring::ByteString;
use codec::{Codec, Packet};
use futures_util::future::BoxFuture;
use service::{client_loop, RemoteAddr, ServiceState};
use storage_memory::MemoryStorage;
use tokio::io::{DuplexStream, ReadHalf, WriteHalf};
use tokio::sync::Mutex;

use crate::suite::{Step, Suite};

struct RunnerContext {
    state: Arc<ServiceState>,
    clients: HashMap<ByteString, Codec<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>>>,
}

pub async fn run(suite: Suite) {
    let state = ServiceState::try_new(suite.config, Box::new(MemoryStorage::default()), Vec::new())
        .await
        .unwrap();
    let ctx = Arc::new(Mutex::new(RunnerContext {
        state,
        clients: HashMap::new(),
    }));

    execute_step(ctx.clone(), suite.step, None, None).await;
    ctx.lock().await.clients.clear();
}

fn execute_step(
    ctx: Arc<Mutex<RunnerContext>>,
    step: Step,
    id: Option<ByteString>,
    client_id: Option<ByteString>,
) -> BoxFuture<'static, ()> {
    let fut = async move {
        match step {
            Step::Connect => {
                let id = id.expect("expect id");
                println!("[CONNECT] id={}", id);
                let mut ctx = ctx.lock().await;
                let (client, server) = tokio::io::duplex(4096);
                let (server_reader, server_writer) = tokio::io::split(server);
                let (client_reader, client_writer) = tokio::io::split(client);
                let codec = Codec::new(client_reader, client_writer);
                tokio::spawn(client_loop(
                    ctx.state.clone(),
                    server_reader,
                    server_writer,
                    RemoteAddr {
                        protocol: "memory",
                        addr: Some(format!("{}", id)),
                    },
                ));
                assert!(
                    ctx.clients.insert(id.clone(), codec).is_none(),
                    "connection id '{}' exists",
                    id
                );
            }
            Step::Disconnect => {
                let id = id.expect("expect id");
                println!("[DISCONNECT] id={}", id);

                let mut ctx = ctx.lock().await;
                assert!(
                    ctx.clients.remove(&id).is_some(),
                    "connection id '{}' not exists",
                    id
                );
            }
            Step::Send { mut packet } => {
                let id = id.clone().expect("expect id");
                println!("[SEND] id={} packet={:?}", id, packet);
                if let Packet::Connect(connect) = &mut packet {
                    connect.client_id = client_id.unwrap_or_else(|| id.clone());
                }
                let mut ctx = ctx.lock().await;
                let codec = ctx
                    .clients
                    .get_mut(&id)
                    .unwrap_or_else(|| panic!("connection id '{}' not exists", id));
                codec.encode(&packet).await.unwrap();
            }
            Step::Receive { packet, after } => {
                let id = id.expect("expect id");
                println!("[RECEIVE] id={} packet={:?}", id, packet);
                let mut ctx = ctx.lock().await;
                let codec = ctx
                    .clients
                    .get_mut(&id)
                    .unwrap_or_else(|| panic!("connection id '{}' not exists", id));

                let recv_packet = if let Some(after) = after {
                    let s = Instant::now();
                    let (recv_packet, _) =
                        tokio::time::timeout(Duration::from_secs(after + 3), codec.decode())
                            .await
                            .expect("receive packet")
                            .unwrap()
                            .expect("unexpected eof");
                    if s.elapsed() < Duration::from_secs(after) {
                        panic!("the message was received within {} seconds.", after);
                    }
                    recv_packet
                } else {
                    let (recv_packet, _) =
                        tokio::time::timeout(Duration::from_secs(3), codec.decode())
                            .await
                            .expect("receive packet")
                            .unwrap()
                            .expect("unexpected eof");
                    recv_packet
                };
                assert_eq!(packet, recv_packet);
            }
            Step::Eof => {
                let id = id.expect("expect id");
                println!("[EOF] id={}", id);
                let mut ctx = ctx.lock().await;
                let codec = ctx
                    .clients
                    .get_mut(&id)
                    .unwrap_or_else(|| panic!("connection id '{}' not exists", id));
                let res = tokio::time::timeout(Duration::from_secs(1), codec.decode())
                    .await
                    .unwrap();
                if !matches!(res, Ok(None)) {
                    panic!("connection is still not closed.")
                }
            }
            Step::Delay { duration } => {
                println!("[DELAY] duration={}", duration);
                tokio::time::sleep(Duration::from_secs(duration)).await
            }
            Step::Parallel { steps } => {
                let mut futs = Vec::new();
                for step in steps {
                    futs.push(execute_step(
                        ctx.clone(),
                        step,
                        id.clone(),
                        client_id.clone(),
                    ));
                }
                futures_util::future::join_all(futs).await;
            }
            Step::Sequence {
                id: new_id,
                client_id,
                steps,
            } => {
                for step in steps {
                    execute_step(
                        ctx.clone(),
                        step,
                        id.clone().or_else(|| new_id.clone()),
                        client_id.clone(),
                    )
                    .await;
                }
            }
        }
    };
    Box::pin(fut)
}
