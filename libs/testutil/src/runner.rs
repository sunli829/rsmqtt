use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytestring::ByteString;
use codec::{Codec, Packet};
use futures_util::future::BoxFuture;
use service::{client_loop, ServiceState, StorageMemory};
use tokio::io::{DuplexStream, ReadHalf, WriteHalf};
use tokio::sync::Mutex;

use crate::suite::{Step, Suite};

struct RunnerContext {
    state: Arc<ServiceState>,
    clients: HashMap<ByteString, Codec<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>>>,
}

pub async fn run(suite: Suite) {
    let state = ServiceState::try_new(suite.config, Box::new(StorageMemory::default()))
        .await
        .unwrap();
    let ctx = Arc::new(Mutex::new(RunnerContext {
        state,
        clients: HashMap::new(),
    }));

    execute_step(ctx.clone(), suite.step, None).await;
    ctx.lock().await.clients.clear();
}

fn execute_step(
    ctx: Arc<Mutex<RunnerContext>>,
    step: Step,
    id: Option<ByteString>,
) -> BoxFuture<'static, ()> {
    let fut = async move {
        match step {
            Step::Connect => {
                let id = id.expect("expect id");
                // println!("[CONNECT] id={}", id);
                let mut ctx = ctx.lock().await;
                let (client, server) = tokio::io::duplex(4096);
                let (server_reader, server_writer) = tokio::io::split(server);
                let (client_reader, client_writer) = tokio::io::split(client);
                let codec = Codec::new(client_reader, client_writer);
                tokio::spawn(client_loop(
                    ctx.state.clone(),
                    server_reader,
                    server_writer,
                    format!("test:{}", id),
                ));
                assert!(
                    ctx.clients.insert(id.clone(), codec).is_none(),
                    "client id '{}' exists",
                    id
                );
            }
            Step::Disconnect => {
                let id = id.expect("expect id");
                // println!("[DISCONNECT] id={}", id);

                let mut ctx = ctx.lock().await;
                assert!(
                    ctx.clients.remove(&id).is_some(),
                    "client id '{}' not exists",
                    id
                );
            }
            Step::Send { mut packet } => {
                let id = id.expect("expect id");
                // println!("[SEND] id={} packet={:?}", id, packet);
                if let Packet::Connect(connect) = &mut packet {
                    connect.client_id = id.clone();
                }
                let mut ctx = ctx.lock().await;
                let codec = ctx
                    .clients
                    .get_mut(&id)
                    .unwrap_or_else(|| panic!("client id '{}' not exists", id));
                codec.encode(&packet).await.unwrap();
            }
            Step::Receive { packet } => {
                let id = id.expect("expect id");
                // println!("[RECEIVE] id={} packet={:?}", id, packet);
                let mut ctx = ctx.lock().await;
                let codec = ctx
                    .clients
                    .get_mut(&id)
                    .unwrap_or_else(|| panic!("client id '{}' not exists", id));
                let (recv_packet, _) = tokio::time::timeout(Duration::from_secs(1), codec.decode())
                    .await
                    .expect("receive packet")
                    .unwrap()
                    .expect("unexpected eof");
                assert_eq!(packet, recv_packet);
            }
            Step::Eof => {
                let id = id.expect("expect id");
                // println!("[EOF] id={}", id);
                let mut ctx = ctx.lock().await;
                let codec = ctx
                    .clients
                    .get_mut(&id)
                    .unwrap_or_else(|| panic!("client id '{}' not exists", id));
                let res = tokio::time::timeout(Duration::from_secs(1), codec.decode())
                    .await
                    .unwrap();
                if !matches!(res, Ok(None)) {
                    panic!("connection is still not closed.")
                }
            }
            Step::Delay { duration } => {
                // println!("[DELAY] duration={}", duration);
                tokio::time::sleep(Duration::from_secs(duration)).await
            }
            Step::Parallel { id: new_id, steps } => {
                let mut futs = Vec::new();
                for step in steps {
                    futs.push(execute_step(
                        ctx.clone(),
                        step,
                        id.clone().or(new_id.clone()),
                    ));
                }
                futures_util::future::join_all(futs).await;
            }
            Step::Sequence { id: new_id, steps } => {
                for step in steps {
                    execute_step(ctx.clone(), step, id.clone().or(new_id.clone())).await;
                }
            }
        }
    };
    Box::pin(fut)
}
