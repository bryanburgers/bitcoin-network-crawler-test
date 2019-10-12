//! Things that are used in top-level message, but are not themselves top-level messages.
//!
//! These are defined to make it very easy to parse/serialize the messages. I'm not entirely happy
//! with the trait, but it seemed to work OK.

use std::convert::TryInto;
use std::io::Write;
use std::net::Ipv6Addr;

/// How to serialize/deserialize a component of a message.
pub trait MessageComponent {
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize>;
    fn to_bytes(&self, stream: &mut impl std::io::Write);
}

impl MessageComponent for bool {
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize> {
        if data.len() < 1 {
            return None;
        };

        if data[0] > 0 {
            *self = true;
        } else {
            *self = false;
        }
        return Some(1);
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        if *self {
            Write::write(stream, &[0x01]).unwrap();
        } else {
            Write::write(stream, &[0x00]).unwrap();
        }
    }
}

impl MessageComponent for i16 {
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize> {
        if data.len() < 2 {
            return None;
        };

        *self = i16::from_le_bytes(data[0..2].try_into().unwrap());

        Some(2)
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        Write::write(stream, &self.to_le_bytes()).unwrap();
    }
}

impl MessageComponent for i32 {
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize> {
        if data.len() < 4 {
            return None;
        };

        *self = i32::from_le_bytes(data[0..4].try_into().unwrap());

        Some(4)
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        Write::write(stream, &self.to_le_bytes()).unwrap();
    }
}

impl MessageComponent for i64 {
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize> {
        if data.len() < 8 {
            return None;
        };

        *self = i64::from_le_bytes(data[0..8].try_into().unwrap());

        Some(8)
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        Write::write(stream, &self.to_le_bytes()).unwrap();
    }
}

impl MessageComponent for u16 {
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize> {
        if data.len() < 2 {
            return None;
        };

        *self = u16::from_le_bytes(data[0..2].try_into().unwrap());

        Some(2)
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        Write::write(stream, &self.to_le_bytes()).unwrap();
    }
}

impl MessageComponent for u32 {
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize> {
        if data.len() < 4 {
            return None;
        };

        *self = u32::from_le_bytes(data[0..4].try_into().unwrap());

        Some(4)
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        Write::write(stream, &self.to_le_bytes()).unwrap();
    }
}

impl MessageComponent for u64 {
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize> {
        if data.len() < 8 {
            return None;
        };

        *self = u64::from_le_bytes(data[0..8].try_into().unwrap());

        Some(8)
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        Write::write(stream, &self.to_le_bytes()).unwrap();
    }
}

/// A variable-sized int. Can be 1, 3, 5, or 9 bytes in length.
#[derive(Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct Varint(pub u64);

impl MessageComponent for Varint {
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize> {
        if data.len() < 1 {
            return None;
        }

        let first_byte = data[0];

        match first_byte {
            0xfd => {
                let mut v = 0_u16;
                if let Some(size) = v.from_bytes(&data[1..]) {
                    self.0 = v as u64;
                    Some(1 + size)
                } else {
                    None
                }
            }
            0xfe => {
                let mut v = 0_u32;
                if let Some(size) = v.from_bytes(&data[1..]) {
                    self.0 = v as u64;
                    Some(1 + size)
                } else {
                    None
                }
            }
            0xff => {
                let mut v = 0_u64;
                if let Some(size) = v.from_bytes(&data[1..]) {
                    self.0 = v as u64;
                    Some(1 + size)
                } else {
                    None
                }
            }
            _ => {
                self.0 = first_byte as u64;
                Some(1)
            }
        }
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        if self.0 < 0xfd {
            stream.write(&[self.0 as u8]).unwrap();
        } else if self.0 <= 0xffff {
            stream.write(&[0xfd]).unwrap();
            stream.write(&(self.0 as u16).to_le_bytes()).unwrap();
        } else if self.0 <= 0xffffffff {
            stream.write(&[0xfe]).unwrap();
            stream.write(&(self.0 as u32).to_le_bytes()).unwrap();
        } else {
            stream.write(&[0xff]).unwrap();
            stream.write(&self.0.to_le_bytes()).unwrap();
        }
    }
}

/// A string is implemented as a Varint that describes how long it is, followed by its content.
impl MessageComponent for String {
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize> {
        let mut length = Varint(0_u64);
        let length_size = length.from_bytes(data)?;
        let length = length.0 as usize;

        if data.len() < length_size + length {
            return None;
        }

        match std::str::from_utf8(&data[length_size..(length_size + length)]) {
            Ok(s) => {
                *self = s.to_string();
                Some(length_size + length)
            }
            Err(_) => None,
        }
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        Varint(self.len() as u64).to_bytes(stream);
        stream.write(&self.as_bytes()).unwrap();
    }
}

/// Information about an address.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Address {
    pub services: u64,
    pub ip: Ipv6Addr,
    pub port: u16,
}

impl Default for Address {
    fn default() -> Self {
        Address {
            services: 0,
            ip: Ipv6Addr::UNSPECIFIED,
            port: 0,
        }
    }
}

// Err... implements to_bytes, but isn't really a standalone message
impl MessageComponent for Address {
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize> {
        if data.len() < (8 + 16 + 2) {
            return None;
        }

        self.services.from_bytes(data);
        let temp: [u8; 16] = data[8..24].try_into().unwrap();
        self.ip = temp.into();
        self.port.from_bytes(&data[24..26]);

        Some(8 + 16 + 2)
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        self.services.to_bytes(stream);
        Write::write(stream, &self.ip.octets()).unwrap();
        self.port.to_bytes(stream);
    }
}

/// A list of items is serialized as a Varint that describes how many items, and then each of the
/// item serialization without separators or terminators.
impl<T> MessageComponent for Vec<T>
where
    T: MessageComponent,
    T: Default,
{
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize> {
        let mut length = Varint(0_u64);
        let mut total_read = 0;
        total_read += length.from_bytes(data)?;
        let length = length.0 as usize;

        for _ in 0..length {
            let mut item: T = Default::default();
            total_read += item.from_bytes(&data[total_read..])?;
            self.push(item);
        }

        Some(total_read)
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        Varint(self.len() as u64).to_bytes(stream);
        for i in self {
            i.to_bytes(stream);
        }
    }
}

/// Sometimes, a collection of two things will be serialized/deserialized.
///
/// For our use, this happens in the `addr` method when a timestamp and an address are serialized
/// together.
impl<T1, T2> MessageComponent for (T1, T2)
where
    T1: MessageComponent,
    T1: Default,
    T2: MessageComponent,
    T2: Default,
{
    fn from_bytes(&mut self, data: &[u8]) -> Option<usize> {
        let mut total_read = 0;
        total_read += self.0.from_bytes(&data[total_read..])?;
        total_read += self.1.from_bytes(&data[total_read..])?;

        Some(total_read)
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        self.0.to_bytes(stream);
        self.1.to_bytes(stream);
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint() {
        let mut received = Vec::new();
        Varint(0x21).to_bytes(&mut received);
        assert_eq!(&received[..], &[0x21]);

        let mut received = Vec::new();
        Varint(0xfd).to_bytes(&mut received);
        assert_eq!(&received[..], &[0xfd, 0xfd, 0x0]);

        let mut received = Vec::new();
        Varint(0x0123).to_bytes(&mut received);
        assert_eq!(&received[..], &[0xfd, 0x23, 0x01]);

        let mut received = Vec::new();
        Varint(0x01234567).to_bytes(&mut received);
        assert_eq!(&received[..], &[0xfe, 0x67, 0x45, 0x23, 0x01]);

        let mut received = Vec::new();
        Varint(0x0123456789abcdef).to_bytes(&mut received);
        assert_eq!(
            &received[..],
            &[0xff, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01]
        );
    }

    #[test]
    fn test_string() {
        let mut received = Vec::new();
        String::from("").to_bytes(&mut received);
        assert_eq!(&received[..], &[0x00]);

        let mut received = Vec::new();
        String::from("bryan").to_bytes(&mut received);
        assert_eq!(&received[..], &[0x05, b'b', b'r', b'y', b'a', b'n']);
    }
}
