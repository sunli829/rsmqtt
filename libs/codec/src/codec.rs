use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

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
        let res = Packet::decode(
            &mut self.reader,
            &mut self.read_buf,
            self.level,
            self.input_max_size,
        )
        .await?;
        if let Some((Packet::Connect(connect), _)) = &res {
            self.level = connect.level;
        }
        Ok(res)
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
