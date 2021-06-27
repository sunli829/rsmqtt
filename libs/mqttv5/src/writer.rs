use bytes::{BufMut, BytesMut};

use crate::EncodeError;

pub trait PacketWriter {
    fn write_remaining_length(&mut self, value: usize) -> Result<(), EncodeError>;

    fn write_string(&mut self, value: &str) -> Result<(), EncodeError>;

    fn write_binary(&mut self, value: &[u8]) -> Result<(), EncodeError>;

    fn write_bool(&mut self, value: bool) -> Result<(), EncodeError>;
}

impl PacketWriter for BytesMut {
    #[inline]
    fn write_remaining_length(&mut self, value: usize) -> Result<(), EncodeError> {
        ensure!(value <= 268_435_455, EncodeError::PayloadTooLarge);

        let mut n = value;

        loop {
            let mut value = (n & 0x7f) as u8;
            n >>= 7;
            if n > 0 {
                value |= 0x80;
            }
            self.put_u8(value);
            if n == 0 {
                break;
            }
        }

        Ok(())
    }

    #[inline]
    fn write_string(&mut self, value: &str) -> Result<(), EncodeError> {
        ensure!(
            value.len() <= u16::MAX as usize,
            EncodeError::PayloadTooLarge
        );
        self.put_u16(value.len() as u16);
        self.put_slice(value.as_bytes());
        Ok(())
    }

    #[inline]
    fn write_binary(&mut self, value: &[u8]) -> Result<(), EncodeError> {
        ensure!(
            value.len() <= u16::MAX as usize,
            EncodeError::PayloadTooLarge
        );
        self.put_u16(value.len() as u16);
        self.put_slice(value);
        Ok(())
    }

    #[inline]
    fn write_bool(&mut self, value: bool) -> Result<(), EncodeError> {
        match value {
            true => self.put_u8(1),
            false => self.put_u8(0),
        }
        Ok(())
    }
}

#[inline]
pub fn bytes_remaining_length(value: usize) -> Result<usize, EncodeError> {
    if value < 128 {
        Ok(1)
    } else if value < 16383 {
        Ok(2)
    } else if value < 2097151 {
        Ok(3)
    } else if value < 268435455 {
        Ok(4)
    } else {
        Err(EncodeError::PayloadTooLarge)
    }
}
