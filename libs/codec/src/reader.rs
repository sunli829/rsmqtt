use std::convert::TryInto;

use bytes::{Buf, Bytes};
use bytestring::ByteString;

use crate::DecodeError;

pub trait PacketReader {
    fn read_u8(&mut self) -> Result<u8, DecodeError>;

    fn read_u16(&mut self) -> Result<u16, DecodeError>;

    fn read_u32(&mut self) -> Result<u32, DecodeError>;

    #[inline]
    fn read_remaining_length(&mut self) -> Result<usize, DecodeError> {
        let mut n = 0;
        let mut shift = 0;

        loop {
            let byte = self.read_u8()?;
            n += ((byte & 0x7f) as usize) << shift;
            let done = (byte & 0x80) == 0;
            if done {
                break;
            }
            shift += 7;
            ensure!(shift <= 21, DecodeError::MalformedPacket);
        }

        Ok(n)
    }

    fn read_string(&mut self) -> Result<ByteString, DecodeError>;

    fn read_binary(&mut self) -> Result<Bytes, DecodeError>;

    #[inline]
    fn read_bool(&mut self) -> Result<bool, DecodeError> {
        Ok(self.read_u8()? > 0)
    }
}

impl PacketReader for Bytes {
    #[inline]
    fn read_u8(&mut self) -> Result<u8, DecodeError> {
        ensure!(self.remaining() >= 1, DecodeError::MalformedPacket);
        Ok(self.get_u8())
    }

    #[inline]
    fn read_u16(&mut self) -> Result<u16, DecodeError> {
        ensure!(self.remaining() >= 2, DecodeError::MalformedPacket);
        Ok(self.get_u16())
    }

    #[inline]
    fn read_u32(&mut self) -> Result<u32, DecodeError> {
        ensure!(self.remaining() >= 4, DecodeError::MalformedPacket);
        Ok(self.get_u32())
    }

    #[inline]
    fn read_string(&mut self) -> Result<ByteString, DecodeError> {
        let len = self.read_u16()? as usize;
        ensure!(self.remaining() >= len, DecodeError::MalformedPacket);
        self.split_to(len)
            .try_into()
            .map_err(|_| DecodeError::MalformedPacket)
    }

    #[inline]
    fn read_binary(&mut self) -> Result<Bytes, DecodeError> {
        let len = self.read_u16()? as usize;
        ensure!(self.remaining() >= len, DecodeError::MalformedPacket);
        Ok(self.split_to(len))
    }
}
