//! All of the top-level messages that we support

use super::component::*;

/// A trait that each message must implement
pub trait MessageTrait: Sized {
    const COMMAND: &'static [u8; 12];
    fn from_bytes(bytes: &[u8]) -> Option<(Self, usize)>;
    fn to_bytes(&self, stream: &mut impl std::io::Write);
}

/// A ping to keep the connection alive
#[derive(Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct Ping {
    pub nonce: u64,
}

impl MessageTrait for Ping {
    const COMMAND: &'static [u8; 12] = b"ping\0\0\0\0\0\0\0\0";

    fn from_bytes(bytes: &[u8]) -> Option<(Self, usize)> {
        let mut ping: Ping = Default::default();

        let mut index = 0;
        index += ping.nonce.from_bytes(bytes)?;

        Some((ping, index))
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        self.nonce.to_bytes(stream);
    }
}

/// A response to a ping
#[derive(Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct Pong {
    pub nonce: u64,
}

impl MessageTrait for Pong {
    const COMMAND: &'static [u8; 12] = b"pong\0\0\0\0\0\0\0\0";

    fn from_bytes(bytes: &[u8]) -> Option<(Self, usize)> {
        let mut pong: Pong = Default::default();

        let mut index = 0;
        index += pong.nonce.from_bytes(bytes)?;

        Some((pong, index))
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        self.nonce.to_bytes(stream);
    }
}

/// Information about addresses a node knows about
#[derive(Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct Addr {
    pub addresses: Vec<(u32, Address)>,
}

impl MessageTrait for Addr {
    const COMMAND: &'static [u8; 12] = b"addr\0\0\0\0\0\0\0\0";

    fn from_bytes(bytes: &[u8]) -> Option<(Self, usize)> {
        let mut addr: Addr = Default::default();

        let mut index = 0;
        index += addr.addresses.from_bytes(bytes)?;

        // assert_eq!(index, bytes.len());

        Some((addr, index))
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        self.addresses.to_bytes(stream);
    }
}

/// Ask for addresses (no data)
#[derive(Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct GetAddr;

impl MessageTrait for GetAddr {
    const COMMAND: &'static [u8; 12] = b"getaddr\0\0\0\0\0";

    fn from_bytes(_bytes: &[u8]) -> Option<(Self, usize)> {
        Some((GetAddr, 0))
    }

    fn to_bytes(&self, _stream: &mut impl std::io::Write) {}
}

/// A version message
#[derive(Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct Version {
    pub version: i32,
    pub services: u64,
    pub timestamp: i64,
    pub addr_recv: Address,
    pub addr_from: Address,
    pub nonce: u64,
    pub user_agent: String,
    pub start_height: i32,
    pub relay: bool,
}

impl MessageTrait for Version {
    const COMMAND: &'static [u8; 12] = b"version\0\0\0\0\0";

    fn from_bytes(bytes: &[u8]) -> Option<(Self, usize)> {
        let mut version: Version = Default::default();

        let mut index = 0;
        index += version.version.from_bytes(&bytes[index..])?;
        index += version.services.from_bytes(&bytes[index..])?;
        index += version.timestamp.from_bytes(&bytes[index..])?;
        index += version.addr_recv.from_bytes(&bytes[index..])?;
        index += version.addr_from.from_bytes(&bytes[index..])?;
        index += version.nonce.from_bytes(&bytes[index..])?;
        index += version.user_agent.from_bytes(&bytes[index..])?;
        index += version.start_height.from_bytes(&bytes[index..])?;
        index += version.relay.from_bytes(&bytes[index..])?;

        Some((version, index))
    }

    fn to_bytes(&self, stream: &mut impl std::io::Write) {
        self.version.to_bytes(stream);
        self.services.to_bytes(stream);
        self.timestamp.to_bytes(stream);
        self.addr_recv.to_bytes(stream);
        self.addr_from.to_bytes(stream);
        self.nonce.to_bytes(stream);
        self.user_agent.to_bytes(stream);
        self.start_height.to_bytes(stream);
        self.relay.to_bytes(stream);
    }
}

/// Acknowledge the version message
#[derive(Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct VerAck;

impl MessageTrait for VerAck {
    const COMMAND: &'static [u8; 12] = b"verack\0\0\0\0\0\0";

    fn from_bytes(_bytes: &[u8]) -> Option<(Self, usize)> {
        Some((VerAck, 0))
    }

    fn to_bytes(&self, _stream: &mut impl std::io::Write) {}
}

/// An alert message
///
/// The docs say this isn't used anymore, but when I had a different version number in the version
/// message, I would get these.
#[derive(Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct Alert {
    payload: AlertPayload,
    // Actual data, not set<uint8_t>
    signature: Vec<u8>,
}

#[derive(Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct AlertPayload {
    version: i32,
    relay_until: i64,
    expiration: i64,
    id: i32,
    cancel: i32,
    set_cancel: Vec<u32>,
    min_ver: i32,
    max_ver: i32,
    set_sub_ver: Vec<String>,
    priority: i32,
    comment: String,
    status_bar: String,
    reserved: String,
}

impl MessageTrait for Alert {
    const COMMAND: &'static [u8; 12] = b"alert\0\0\0\0\0\0\0";

    fn from_bytes(bytes: &[u8]) -> Option<(Self, usize)> {
        let mut alert: Alert = Default::default();
        let mut alert_payload: AlertPayload = Default::default();

        let mut index = 0;
        let mut alert_length = Varint(0);
        index += alert_length.from_bytes(&bytes[index..])?;
        index += alert_payload.version.from_bytes(&bytes[index..])?;
        index += alert_payload.relay_until.from_bytes(&bytes[index..])?;
        index += alert_payload.expiration.from_bytes(&bytes[index..])?;
        index += alert_payload.id.from_bytes(&bytes[index..])?;
        index += alert_payload.cancel.from_bytes(&bytes[index..])?;
        index += alert_payload.set_cancel.from_bytes(&bytes[index..])?;
        index += alert_payload.min_ver.from_bytes(&bytes[index..])?;
        index += alert_payload.max_ver.from_bytes(&bytes[index..])?;
        index += alert_payload.set_sub_ver.from_bytes(&bytes[index..])?;
        index += alert_payload.priority.from_bytes(&bytes[index..])?;
        index += alert_payload.comment.from_bytes(&bytes[index..])?;
        index += alert_payload.status_bar.from_bytes(&bytes[index..])?;
        index += alert_payload.reserved.from_bytes(&bytes[index..])?;
        alert.signature = Vec::new();
        alert.signature.extend_from_slice(&bytes[index..]);
        alert.payload = alert_payload;

        Some((alert, index))
    }

    fn to_bytes(&self, _stream: &mut impl std::io::Write) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_alert() {
        let data = vec![
            96, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 127, 0, 0, 0, 0, 255, 255, 255,
            127, 254, 255, 255, 127, 1, 255, 255, 255, 127, 0, 0, 0, 0, 255, 255, 255, 127, 0, 255,
            255, 255, 127, 0, 47, 85, 82, 71, 69, 78, 84, 58, 32, 65, 108, 101, 114, 116, 32, 107,
            101, 121, 32, 99, 111, 109, 112, 114, 111, 109, 105, 115, 101, 100, 44, 32, 117, 112,
            103, 114, 97, 100, 101, 32, 114, 101, 113, 117, 105, 114, 101, 100, 0, 70, 48, 68, 2,
            32, 101, 63, 235, 214, 65, 15, 71, 15, 107, 174, 17, 202, 209, 156, 72, 65, 59, 236,
            177, 172, 44, 23, 249, 8, 253, 15, 213, 59, 220, 58, 189, 82, 2, 32, 109, 14, 156, 150,
            254, 136, 212, 160, 240, 30, 217, 222, 218, 226, 182, 249, 224, 13, 169, 76, 173, 15,
            236, 170, 230, 110, 207, 104, 155, 247, 27, 80,
        ];

        let alert = Alert::from_bytes(&data);
        assert!(alert.is_some());
    }
}
