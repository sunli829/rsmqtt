use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ErrorKind};

use crate::{
    ConnAck, Connect, Disconnect, PubAck, PubComp, PubRec, PubRel, Publish, SubAck, Subscribe,
    UnsubAck, Unsubscribe,
};
use crate::{DecodeError, EncodeError};

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

#[derive(Debug)]
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
    pub async fn decode(
        mut reader: impl AsyncRead + Unpin,
        data: &mut BytesMut,
        max_size: Option<u32>,
    ) -> Result<Option<(Self, usize)>, DecodeError> {
        let flag = match reader.read_u8().await {
            Ok(flag) => flag,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let mut packet_size = 1;
        let (len, bytes_len) = read_remaining_length(&mut reader).await?;

        packet_size += bytes_len;

        if matches!(max_size, Some(max_size) if len > max_size as usize) {
            return Err(DecodeError::PacketTooLarge);
        }

        data.resize(len, 0);
        reader
            .read_exact(&mut *data)
            .await
            .map_err(|_| DecodeError::MalformedPacket)?;
        packet_size += data.len();

        let packet = match (flag & 0xf0) >> 4 {
            RESERVED => return Err(DecodeError::ReservedPacketType),
            CONNECT => Self::Connect(Connect::decode(data.split().freeze())?),
            CONNACK => Self::ConnAck(ConnAck::decode(data.split().freeze())?),
            PUBLISH => Self::Publish(Publish::decode(data.split().freeze(), flag)?),
            PUBACK => Self::PubAck(PubAck::decode(data.split().freeze())?),
            PUBREC => Self::PubRec(PubRec::decode(data.split().freeze())?),
            PUBREL => Self::PubRel(PubRel::decode(data.split().freeze(), flag)?),
            PUBCOMP => Self::PubComp(PubComp::decode(data.split().freeze())?),
            SUBSCRIBE => Self::Subscribe(Subscribe::decode(data.split().freeze(), flag)?),
            SUBACK => Self::SubAck(SubAck::decode(data.split().freeze())?),
            UNSUBSCRIBE => Self::Unsubscribe(Unsubscribe::decode(data.split().freeze(), flag)?),
            UNSUBACK => Self::UnsubAck(UnsubAck::decode(data.split().freeze())?),
            PINGREQ => Self::PingReq,
            PINGRESP => Self::PingResp,
            DISCONNECT => Self::Disconnect(Disconnect::decode(data.split().freeze())?),
            n => return Err(DecodeError::UnknownPacketType(n)),
        };
        Ok(Some((packet, packet_size)))
    }

    fn encode(&self, data: &mut BytesMut) -> Result<(), EncodeError> {
        match self {
            Packet::Connect(connect) => connect.encode(data),
            Packet::ConnAck(conn_ack) => conn_ack.encode(data),
            Packet::Publish(publish) => publish.encode(data),
            Packet::PubAck(pub_ack) => pub_ack.encode(data),
            Packet::PubRec(pub_rec) => pub_rec.encode(data),
            Packet::PubRel(pub_rel) => pub_rel.encode(data),
            Packet::PubComp(pub_comp) => pub_comp.encode(data),
            Packet::Subscribe(subscribe) => subscribe.encode(data),
            Packet::SubAck(sub_ack) => sub_ack.encode(data),
            Packet::Unsubscribe(unsubscribe) => unsubscribe.encode(data),
            Packet::UnsubAck(unsub_ack) => unsub_ack.encode(data),
            Packet::PingReq => {
                data.put_slice(&[PINGREQ, 0]);
                Ok(())
            }
            Packet::PingResp => {
                data.put_slice(&[PINGRESP, 0]);
                Ok(())
            }
            Packet::Disconnect(disconnect) => disconnect.encode(data),
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

pub struct PacketEncoder<W> {
    writer: W,
    data: BytesMut,
    max_size: Option<u32>,
}

impl<W> PacketEncoder<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            data: BytesMut::new(),
            max_size: None,
        }
    }

    pub fn set_max_size(&mut self, max_size: u32) {
        self.max_size = Some(max_size);
    }
}

impl<W: AsyncWrite + Unpin> PacketEncoder<W> {
    pub async fn encode(&mut self, packet: &Packet) -> Result<usize, EncodeError> {
        packet.encode(&mut self.data)?;
        let data = self.data.split();
        let packet_size = data.len();
        if let Some(max_size) = self.max_size {
            if data.len() > max_size as usize {
                return Err(EncodeError::PacketTooLarge);
            }
        }
        self.writer.write_all(&*data).await?;
        Ok(packet_size)
    }
}
