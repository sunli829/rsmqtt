use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ErrorKind};

use crate::{DecodeError, EncodeError, Level, Packet};

pub struct Codec<R, W> {
    reader: R,
    writer: W,
    level: Level,
    input_max_size: usize,
    output_max_size: usize,
    read_buf: BytesMut,
    write_buf: BytesMut,
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
            level: Level::V4,
            input_max_size: usize::MAX,
            output_max_size: usize::MAX,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
        }
    }

    pub fn set_input_max_size(&mut self, size: usize) {
        self.input_max_size = size;
    }

    pub fn set_output_max_size(&mut self, size: usize) {
        self.output_max_size = size;
    }

    pub async fn decode(&mut self) -> Result<Option<(Packet, usize)>, DecodeError> {
        let flag = match self.reader.read_u8().await {
            Ok(flag) => flag,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let (len, packet_size) = read_remaining_length(&mut self.reader).await?;
        if len > self.input_max_size {
            return Err(DecodeError::PacketTooLarge);
        }
        self.read_buf.resize(len, 0);
        self.reader.read_exact(&mut self.read_buf[..]).await?;

        let packet = Packet::decode(self.read_buf.split().freeze(), flag, self.level)?;
        if let Packet::Connect(connect) = &packet {
            self.level = connect.level;
        }
        Ok(Some((packet, packet_size)))
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
