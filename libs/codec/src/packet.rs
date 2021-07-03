use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, ErrorKind};

use crate::{
    ConnAck, Connect, DecodeError, Disconnect, EncodeError, Level, PubAck, PubComp, PubRec, PubRel,
    Publish, SubAck, Subscribe, UnsubAck, Unsubscribe,
};

pub const RESERVED: u8 = 0;
pub const CONNECT: u8 = 1;
pub const CONNACK: u8 = 2;
pub const PUBLISH: u8 = 3;
pub const PUBACK: u8 = 4;
pub const PUBREC: u8 = 5;
pub const PUBREL: u8 = 6;
pub const PUBCOMP: u8 = 7;
pub const SUBSCRIBE: u8 = 8;
pub const SUBACK: u8 = 9;
pub const UNSUBSCRIBE: u8 = 10;
pub const UNSUBACK: u8 = 11;
pub const PINGREQ: u8 = 12;
pub const PINGRESP: u8 = 13;
pub const DISCONNECT: u8 = 14;
// const AUTH: u8 = 15;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Packet {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(PubAck),
    PubRec(PubRec),
    PubRel(PubRel),
    PubComp(PubComp),
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    PingReq,
    PingResp,
    Disconnect(Disconnect),
}

impl Packet {
    pub(crate) async fn decode(
        mut reader: impl AsyncRead + Unpin,
        data: &mut BytesMut,
        level: Level,
        max_size: usize,
    ) -> Result<Option<(Self, usize)>, DecodeError> {
        let flag = match reader.read_u8().await {
            Ok(flag) => flag,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let mut packet_size = 1;
        let (len, bytes_len) = read_remaining_length(&mut reader).await?;
        ensure!(len <= max_size, DecodeError::PacketTooLarge);

        packet_size += bytes_len;

        data.resize(len, 0);
        reader
            .read_exact(&mut *data)
            .await
            .map_err(|_| DecodeError::MalformedPacket)?;
        packet_size += data.len();

        let packet = match (flag & 0xf0) >> 4 {
            RESERVED => return Err(DecodeError::ReservedPacketType),
            CONNECT => Self::Connect(Connect::decode(data.split().freeze(), level)?),
            CONNACK => Self::ConnAck(ConnAck::decode(data.split().freeze(), level)?),
            PUBLISH => Self::Publish(Publish::decode(data.split().freeze(), level, flag)?),
            PUBACK => Self::PubAck(PubAck::decode(data.split().freeze(), level)?),
            PUBREC => Self::PubRec(PubRec::decode(data.split().freeze(), level)?),
            PUBREL => Self::PubRel(PubRel::decode(data.split().freeze(), level, flag)?),
            PUBCOMP => Self::PubComp(PubComp::decode(data.split().freeze(), level)?),
            SUBSCRIBE => Self::Subscribe(Subscribe::decode(data.split().freeze(), level, flag)?),
            SUBACK => Self::SubAck(SubAck::decode(data.split().freeze(), level)?),
            UNSUBSCRIBE => {
                Self::Unsubscribe(Unsubscribe::decode(data.split().freeze(), level, flag)?)
            }
            UNSUBACK => Self::UnsubAck(UnsubAck::decode(data.split().freeze(), level)?),
            PINGREQ => Self::PingReq,
            PINGRESP => Self::PingResp,
            DISCONNECT => Self::Disconnect(Disconnect::decode(data.split().freeze(), level)?),
            n => return Err(DecodeError::UnknownPacketType(n)),
        };
        Ok(Some((packet, packet_size)))
    }

    pub(crate) fn encode(
        &self,
        data: &mut BytesMut,
        level: Level,
        max_size: usize,
    ) -> Result<(), EncodeError> {
        match self {
            Packet::Connect(connect) => connect.encode(data, level, max_size),
            Packet::ConnAck(conn_ack) => conn_ack.encode(data, level, max_size),
            Packet::Publish(publish) => publish.encode(data, level, max_size),
            Packet::PubAck(pub_ack) => pub_ack.encode(data, level, max_size),
            Packet::PubRec(pub_rec) => pub_rec.encode(data, level, max_size),
            Packet::PubRel(pub_rel) => pub_rel.encode(data, level, max_size),
            Packet::PubComp(pub_comp) => pub_comp.encode(data, level, max_size),
            Packet::Subscribe(subscribe) => subscribe.encode(data, level, max_size),
            Packet::SubAck(sub_ack) => sub_ack.encode(data, level, max_size),
            Packet::Unsubscribe(unsubscribe) => unsubscribe.encode(data, level, max_size),
            Packet::UnsubAck(unsub_ack) => unsub_ack.encode(data, level, max_size),
            Packet::PingReq => {
                data.put_slice(&[PINGREQ << 4, 0]);
                Ok(())
            }
            Packet::PingResp => {
                data.put_slice(&[PINGRESP << 4, 0]);
                Ok(())
            }
            Packet::Disconnect(disconnect) => disconnect.encode(data, level, max_size),
        }
    }
}

async fn read_remaining_length(
    mut reader: impl AsyncRead + Unpin,
) -> Result<(usize, usize), DecodeError> {
    let mut n = 0;
    let mut shift = 0;
    let mut bytes = 0;

    loop {
        let byte = reader.read_u8().await?;
        bytes += 1;
        n += ((byte & 0x7f) as usize) << shift;
        let done = (byte & 0x80) == 0;
        if done {
            break;
        }
        shift += 7;
        ensure!(shift <= 21, DecodeError::MalformedPacket);
    }

    Ok((n, bytes))
}
