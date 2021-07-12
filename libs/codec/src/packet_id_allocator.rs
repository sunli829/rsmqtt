use std::convert::TryInto;
use std::num::NonZeroU16;

pub struct PacketIdAllocator(u16);

impl Default for PacketIdAllocator {
    #[inline]
    fn default() -> Self {
        Self(1.try_into().unwrap())
    }
}

impl PacketIdAllocator {
    #[inline]
    pub fn take(&mut self) -> NonZeroU16 {
        let id = self.0;
        if self.0 == u16::MAX {
            self.0 = 1;
        } else {
            self.0 += 1;
        }
        id.try_into().unwrap()
    }

    #[inline]
    pub fn reset(&mut self) {
        *self = PacketIdAllocator::default();
    }
}
