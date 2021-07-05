#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

use std::convert::TryInto;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use bytesize::ByteSize;
use bytestring::ByteString;
use codec::{
    Codec, Connect, ConnectProperties, ConnectReasonCode, Level, Packet, Publish,
    PublishProperties, Qos,
};
use structopt::StructOpt;
use tokio::net::TcpStream;
use tokio::sync::Barrier;

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

    let mut count = 0;
    for handle in handles {
        match handle.await.unwrap() {
            Ok(res) => count += res,
            Err(err) => {
                println!("error: {}", err);
                break;
            }
        }
    }

    println!("TPS: {:.3}", count as f64 / options.duration as f64);
    println!(
        "Transferred Bytes: {}",
        ByteSize::b((count * options.payload_size) as u64)
    );
}

async fn client_loop(
    id: usize,
    barrier: Arc<Barrier>,
    addr: (String, u16),
    payload: Bytes,
    duration: usize,
) -> Result<usize> {
    let mut stream = TcpStream::connect(addr).await?;
    let (reader, writer) = stream.split();
    let mut codec = Codec::new(reader, writer);
    let client_id = format!("client{}", id).into();
    let topic: ByteString = format!("client{}", id).into();

    // connect
    codec
        .encode(&Packet::Connect(Connect {
            level: Level::V5,
            keep_alive: 60,
            clean_start: true,
            client_id,
            last_will: None,
            login: None,
            properties: ConnectProperties::default(),
        }))
        .await?;

    let (packet, _) = codec
        .decode()
        .await?
        .ok_or_else(|| anyhow::anyhow!("protocol error"))?;
    let conn_ack = match packet {
        Packet::ConnAck(conn_ack) => conn_ack,
        _ => anyhow::bail!("protocol error"),
    };

    anyhow::ensure!(
        conn_ack.reason_code == ConnectReasonCode::Success,
        "failed to connect to mqtt server: {:?}",
        conn_ack.reason_code
    );

    barrier.wait().await;

    let mut packet_id = 1u16;
    let start_time = Instant::now();
    let mut count = 0;

    loop {
        if start_time.elapsed() > Duration::from_secs(duration as u64) {
            break;
        }

        codec
            .encode(&Packet::Publish(Publish {
                dup: false,
                qos: Qos::AtLeastOnce,
                retain: false,
                topic: topic.clone(),
                packet_id: Some(packet_id.try_into().unwrap()),
                properties: PublishProperties::default(),
                payload: payload.clone(),
            }))
            .await?;

        let (packet, _) = codec
            .decode()
            .await?
            .ok_or_else(|| anyhow::anyhow!("protocol error"))?;
        match packet {
            Packet::PubAck(_) => {}
            _ => anyhow::bail!("protocol error"),
        };

        if packet_id < u16::MAX {
            packet_id += 1;
        } else {
            packet_id = 1;
        }

        count += 1;
    }

    Ok(count)
}
