use bytes::{Buf, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion};
use rsmqtt_codec::{Packet, ProtocolLevel, Publish, PublishProperties, Qos};

fn encode_publish(c: &mut Criterion) {
    let packet = Packet::Publish(Publish {
        dup: false,
        qos: Qos::AtMostOnce,
        retain: false,
        topic: "abcdefg".into(),
        packet_id: None,
        properties: PublishProperties::default(),
        payload: "abcdefgabcdefgabcdefgabcdefgabcdefgabcdefg".into(),
    });
    let mut buf = BytesMut::new();

    c.bench_function("encode publish", |b| {
        b.iter(|| {
            buf.clear();
            Packet::encode(&packet, &mut buf, ProtocolLevel::V5, usize::MAX).unwrap();
        });
    });
}

fn decode_publish(c: &mut Criterion) {
    let packet = Packet::Publish(Publish {
        dup: false,
        qos: Qos::AtMostOnce,
        retain: false,
        topic: "abcdefg".into(),
        packet_id: None,
        properties: PublishProperties::default(),
        payload: "abcdefgabcdefgabcdefgabcdefgabcdefgabcdefg".into(),
    });
    let mut buf = BytesMut::new();
    Packet::encode(&packet, &mut buf, ProtocolLevel::V5, usize::MAX).unwrap();
    let mut packet_data = buf.freeze();
    let flag = packet_data[0];
    packet_data.advance(2);

    c.bench_function("decode publish", |b| {
        b.iter(|| {
            Packet::decode(packet_data.clone(), flag, ProtocolLevel::V5).unwrap();
        });
    });
}

criterion_group!(benches, encode_publish, decode_publish);
criterion_main!(benches);
