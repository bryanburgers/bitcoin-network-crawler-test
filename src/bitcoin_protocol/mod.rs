//! Everything about the bitcoin protocol that we need to carry out the work
//!
//! This is very much the product of hacking things together. I haven't gone back through and made
//! everything make more sense, including the traits.

pub mod component;
pub mod messages;
mod stream_reader;
mod stream_writer;

use component::*;
pub use messages::*;
pub use stream_reader::{StreamReader, StreamReaderError};
pub use stream_writer::StreamWriter;

/// A single message to send across the connection in either direction.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Message {
    /// The type of the message
    pub command: MessageName,
    /// The payload of the message
    pub payload: MessagePayload,
}

impl Message {
    /// Create a version message
    pub fn version(version: messages::Version) -> Self {
        Message {
            command: messages::Version::COMMAND.into(),
            payload: MessagePayload::Version(version),
        }
    }

    /// Create a verack message
    pub fn verack() -> Self {
        Message {
            command: messages::VerAck::COMMAND.into(),
            payload: MessagePayload::VerAck(messages::VerAck),
        }
    }

    /*
    /// Create a ping message
    pub fn ping(nonce: u64) -> Self {
        Message {
            command: messages::Ping::COMMAND.into(),
            payload: MessagePayload::Ping(messages::Ping { nonce }),
        }
    }
    */

    /// Create a pong message
    pub fn pong(nonce: u64) -> Self {
        Message {
            command: messages::Pong::COMMAND.into(),
            payload: MessagePayload::Pong(messages::Pong { nonce }),
        }
    }

    /// Create a getaddr message
    pub fn getaddr() -> Self {
        Message {
            command: messages::GetAddr::COMMAND.into(),
            payload: MessagePayload::GetAddr(messages::GetAddr),
        }
    }
}

/// The various types of message that we support.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum MessagePayload {
    Version(messages::Version),
    VerAck(messages::VerAck),
    Alert(messages::Alert),
    Addr(messages::Addr),
    Ping(messages::Ping),
    Pong(messages::Pong),
    GetAddr(messages::GetAddr),
    /// We received a message that we don't support. That's fine. We just probably won't do
    /// anything with it.
    Unknown(Vec<u8>),
}

impl MessagePayload {
    /// Serialize the message payload to bytes.
    fn to_bytes(&self, data: &mut Vec<u8>) {
        match self {
            Self::Version(version) => version.to_bytes(data),
            Self::VerAck(verack) => verack.to_bytes(data),
            Self::Alert(alert) => alert.to_bytes(data),
            Self::Addr(addr) => addr.to_bytes(data),
            Self::Ping(ping) => ping.to_bytes(data),
            Self::Pong(pong) => pong.to_bytes(data),
            Self::GetAddr(getaddr) => getaddr.to_bytes(data),
            Self::Unknown(unknown) => {
                std::io::Write::write(data, &unknown[..]).unwrap();
            }
        }
    }
}

/// Which network to communicate on.
///
/// Each bitcoin network sends different header data, so we need to know which we are connected to.
#[derive(Debug, Clone)]
pub enum Network {
    Main,
    Testnet,
    Testnet3,
    Namecoin,
}

const MAIN_HEADER: &'static [u8; 4] = &[0xf9, 0xbe, 0xb4, 0xd9];
const TESTNET_HEADER: &'static [u8; 4] = &[0xfa, 0xbf, 0xb5, 0xda];
const TESTNET3_HEADER: &'static [u8; 4] = &[0x0b, 0x11, 0x09, 0x07];
const NAMECOIN_HEADER: &'static [u8; 4] = &[0xf9, 0xbe, 0xb4, 0xfe];

impl Network {
    /// Get the 4-byte magic number used in the header
    pub fn header(&self) -> &[u8; 4] {
        match *self {
            Self::Main => MAIN_HEADER,
            Self::Testnet => TESTNET_HEADER,
            Self::Testnet3 => TESTNET3_HEADER,
            Self::Namecoin => NAMECOIN_HEADER,
        }
    }

    /// Get the port that is typically used for this network
    pub fn default_port(&self) -> u16 {
        match *self {
            Self::Main => 8333,
            Self::Testnet => 18333,
            Self::Testnet3 => 18333,
            Self::Namecoin => 8334,
        }
    }
}

impl MessageComponent for Network {
    fn from_bytes(&mut self, _data: &[u8]) -> Option<usize> {
        // TODO: Parse this!
        None
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        stream.write(self.header()).unwrap();
    }
}

/// The command, or the message name.
///
/// The thing that describes which message is being sent or received.
///
/// The message name is always 12 bytes long, and if the ascii is less than 12 bytes, it's padded
/// with null bytes.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct MessageName([u8; 12]);

impl From<&[u8; 12]> for MessageName {
    fn from(v: &[u8; 12]) -> Self {
        let mut a: [u8; 12] = Default::default();
        a.copy_from_slice(&v[0..12]);
        MessageName(a)
    }
}

impl MessageComponent for MessageName {
    fn from_bytes(&mut self, _data: &[u8]) -> Option<usize> {
        // TODO: parse
        None
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        stream.write(&self.0).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        let version = Version {
            version: 31900,
            services: 1,
            timestamp: 0x4d1015e6,
            addr_recv: Address {
                services: 1,
                ip: "::ffff:10.0.0.1".parse().unwrap(),
                port: 36128,
            },
            addr_from: Address {
                services: 1,
                ip: "::ffff:10.0.0.2".parse().unwrap(),
                port: 36128,
            },
            nonce: 0x1357b43a2c209ddd,
            user_agent: "".to_string(),
            start_height: 98645,
            relay: false,
        };
        let mut received = Vec::new();
        let expected = vec![
            0x9C, 0x7C, 0x00, 0x00, // - 31900 (version 0.3.19)
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // - 1 (NODE_NETWORK services)
            0xE6, 0x15, 0x10, 0x4D, 0x00, 0x00, 0x00, 0x00, // - Mon Dec 20 21:50:14 EST 2010
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x0A, 0x00, 0x00, 0x01, 0x20,
            0x8D, // - Recipient address info - see Network Address
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x0A, 0x00, 0x00, 0x02, 0x20,
            0x8D, // - Sender address info - see Network Address
            0xDD, 0x9D, 0x20, 0x2C, 0x3A, 0xB4, 0x57, 0x13, // - Node random unique ID
            0x00, // - "" sub-version string (string is 0 bytes long)
            0x55, 0x81, 0x01, 0x00, // - Last block sending node has is block #98645
            0x00, // - Relay
        ];
        version.to_bytes(&mut received);
        assert_eq!(&received[..], &expected[..]);
    }
}
