use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{DecodeError, EncodeError, Packet, ProtocolLevel};

#[derive(Debug, Copy, Clone)]
enum DecoderState {
    Flag,
    Length(u8),
    Body(u8, usize),
}

pub struct Codec<R, W> {
    reader: R,
    writer: W,
    level: ProtocolLevel,
    input_max_size: usize,
    output_max_size: usize,
    read_buf: BytesMut,
    write_buf: BytesMut,
    decoder_state: DecoderState,
}

impl<R, W> Codec<R, W>
where
    R: AsyncRead + Send + Unpin,
    W: AsyncWrite + Send + Unpin,
{
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader,
            writer,
            level: ProtocolLevel::V4,
            input_max_size: usize::MAX,
            output_max_size: usize::MAX,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            decoder_state: DecoderState::Flag,
        }
    }

    #[inline]
    pub fn protocol_level(&self) -> ProtocolLevel {
        self.level
    }

    #[inline]
    pub fn set_input_max_size(&mut self, size: usize) {
        self.input_max_size = size;
    }

    #[inline]
    pub fn set_output_max_size(&mut self, size: usize) {
        self.output_max_size = size;
    }

    pub async fn decode(&mut self) -> Result<Option<(Packet, usize)>, DecodeError> {
        let mut data = [0; 256];

        loop {
            match self.decoder_state {
                DecoderState::Flag => {
                    if !self.read_buf.is_empty() {
                        self.decoder_state = DecoderState::Length(self.read_buf.get_u8());
                        continue;
                    }
                }
                DecoderState::Length(flag) => {
                    if let Some((packet_size, len_size)) = get_remaining_length(&self.read_buf)? {
                        if packet_size > self.input_max_size {
                            return Err(DecodeError::PacketTooLarge);
                        }
                        self.read_buf.advance(len_size);
                        self.decoder_state = DecoderState::Body(flag, packet_size);
                        continue;
                    }
                }
                DecoderState::Body(flag, packet_size) => {
                    if self.read_buf.len() >= packet_size {
                        let data = self.read_buf.split_to(packet_size).freeze();
                        self.decoder_state = DecoderState::Flag;
                        let packet = Packet::decode(data, flag, self.level)?;
                        if let Packet::Connect(connect) = &packet {
                            self.level = connect.level;
                        }
                        return Ok(Some((packet, packet_size)));
                    }
                }
            }

            let sz = self.reader.read(&mut data).await?;
            if sz == 0 {
                return match self.decoder_state {
                    DecoderState::Flag => Ok(None),
                    DecoderState::Length(_) | DecoderState::Body(_, _) => {
                        Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into())
                    }
                };
            }
            self.read_buf.extend_from_slice(&data[..sz]);
        }
    }

    pub async fn encode(&mut self, packet: &Packet) -> Result<usize, EncodeError> {
        if let Packet::Connect(connect) = &packet {
            self.level = connect.level;
        }
        packet.encode(&mut self.write_buf, self.level, self.output_max_size)?;
        self.writer.write_all(&self.write_buf).await?;
        let size = self.write_buf.len();
        self.write_buf.clear();
        Ok(size)
    }
}

#[inline]
fn get_remaining_length(data: &[u8]) -> Result<Option<(usize, usize)>, DecodeError> {
    let mut n = 0;
    let mut shift = 0;
    let mut bytes = 0;

    for i in 0.. {
        if i >= data.len() {
            return Ok(None);
        }

        let byte = data[i];
        bytes += 1;
        n += ((byte & 0x7f) as usize) << shift;
        let done = (byte & 0x80) == 0;
        if done {
            break;
        }
        shift += 7;
        ensure!(shift <= 21, DecodeError::MalformedPacket);
    }

    Ok(Some((n, bytes)))
}
