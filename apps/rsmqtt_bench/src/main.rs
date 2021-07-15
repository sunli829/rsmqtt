#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use bytesize::ByteSize;
use bytestring::ByteString;
use client::{Client, FilterBuilder, Qos};
use structopt::StructOpt;
use tokio::sync::Barrier;
use tokio_stream::StreamExt;

#[derive(StructOpt)]
struct Options {
    /// mqtt host to connect to.
    #[structopt(default_value = "localhost", short)]
    pub host: String,

    /// network port to connect to.
    #[structopt(default_value = "1883", short)]
    pub port: u16,

    /// number of threads to use.
    #[structopt(name = "threads", default_value = "32", short = "t")]
    pub num_threads: usize,

    /// payload size to publish.
    #[structopt(name = "payload_size", default_value = "256", short = "s")]
    pub payload_size: usize,

    /// duration of test
    #[structopt(default_value = "10", short = "d")]
    pub duration: usize,
}

#[tokio::main]
async fn main() {
    let options: Options = Options::from_args();
    let payload: Bytes = b"123456789"
        .iter()
        .copied()
        .cycle()
        .take(options.payload_size)
        .collect();
    let barrier = Arc::new(Barrier::new(options.num_threads + 1));
    let mut handles = Vec::new();

    for i in 0..options.num_threads {
        let handle = tokio::spawn(client_loop(
            i,
            barrier.clone(),
            (options.host.clone(), options.port),
            payload.clone(),
            options.duration,
        ));
        handles.push(handle);
    }

    barrier.wait().await;

    println!("connected");

    let mut send_count = 0;
    let mut recv_count = 0;

    for handle in handles {
        match handle.await.unwrap() {
            Ok(res) => {
                send_count += res.0;
                recv_count += res.1;
            }
            Err(err) => {
                println!("error: {}", err);
                break;
            }
        }
    }

    println!(
        "Send TPS: {:.3}",
        send_count as f64 / options.duration as f64
    );
    println!(
        "Receive TPS: {:.3}",
        recv_count as f64 / options.duration as f64
    );
    println!(
        "Transferred Bytes: {}",
        ByteSize::b(((send_count + recv_count) * options.payload_size) as u64)
    );
}

async fn client_loop(
    id: usize,
    barrier: Arc<Barrier>,
    addr: (String, u16),
    payload: Bytes,
    duration: usize,
) -> Result<(usize, usize)> {
    let (client, mut receiver) = Client::new(addr)
        .client_id(format!("client{}", id))
        .clean_start()
        .build()
        .await
        .unwrap();
    let topic: ByteString = format!("client{}", id).into();
    client
        .subscribe()
        .filter(FilterBuilder::new(topic.clone()))
        .send()
        .await
        .unwrap();

    barrier.wait().await;

    let send_count = Arc::new(AtomicUsize::default());
    let recv_count = Arc::new(AtomicUsize::default());

    let timeout = tokio::time::sleep(Duration::from_secs(duration as u64));
    let publish_task = {
        let send_count = send_count.clone();
        async move {
            loop {
                client
                    .publish(topic.clone())
                    .qos(Qos::ExactlyOnce)
                    .payload(payload.clone())
                    .send()
                    .await
                    .unwrap();
                send_count.fetch_add(1, Ordering::SeqCst);
            }
        }
    };
    let receive_task = {
        let recv_count = recv_count.clone();
        async move {
            while let Some(_) = receiver.next().await {
                recv_count.fetch_add(1, Ordering::SeqCst);
            }
        }
    };

    tokio::select! {
        _ = timeout => {}
        _ = publish_task => {}
        _ = receive_task => {}
    }

    Ok((
        send_count.load(Ordering::SeqCst),
        recv_count.load(Ordering::SeqCst),
    ))
}
