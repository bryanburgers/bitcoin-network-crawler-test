use super::MessageName;
use super::Network;
use super::{Message, MessagePayload};
use super::messages::{self, MessageTrait};
use async_std::{
    stream::Stream,
    task::{Context, Poll},
};
use futures::io::AsyncRead;
use std::convert::TryInto;
use std::marker::Unpin;
use std::pin::Pin;
use std::ops::Range;

/// A stream reader that reads data coming across the TCP stream, and gives off the messages it
/// parses.
pub struct StreamReader<T> {
    /// Which bitcoin network we're on (so we know if the messages ar valid)
    network: Network,
    /// The TCP Stream
    reader: T,
    /// A buffer of data that we use for the stream's `read` method. We keep it around in case
    /// we received a partial message, and so we don't have to reallocate every time.
    buffer: Option<Vec<u8>>,
    /// Because we keep our buffer around all of the time, read_bytes describes which bytes have
    /// been read and are valid.
    read_bytes: Range<usize>,
    /// Whether the connection has closed. Once it has, don't even try to do stuff, just return
    /// that it's closed.
    is_done: bool,
}

/// Errors that can occur.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum StreamReaderError {
    NotEnoughData,
    InvalidHeader,
}

impl<T> StreamReader<T> {
    /// Create a new stram reader!
    pub fn new(network: Network, reader: T) -> Self {
        StreamReader {
            network: network,
            reader,
            buffer: Some(vec![0; 32768]),
            read_bytes: 0..0,
            is_done: false,
        }
    }

    /// Parse a single message out of a buffer
    ///
    /// Returns None if we need more data in the buffer to make a full message
    /// Returns Some(message, size) if we parsed a single full message. size is how many bytes were
    /// in that message.
    /// TODO: We probably also need to signal if the data doesn't look like a message
    fn parse(&self, buffer: &[u8]) -> Result<(Message, usize), StreamReaderError> {
        if buffer.len() < 24 {
            // We don't have enough bytes for a header yet.
            Err(StreamReaderError::NotEnoughData)?
        }

        let header = &buffer[0..4];
        let command_bytes = &buffer[4..16];
        let length = &buffer[16..20];
        let _checksum = &buffer[20..24];

        if header != self.network.header() {
            println!("header={:?}", header);
            println!("full={:?}", buffer);
            Err(StreamReaderError::InvalidHeader)?
        }
        let command = match command_bytes.try_into() {
            Err(e) => panic!("Failed to parse 12 bytes into a message name: {}", e),
            Ok(command) => MessageName(command),
        };

        let length = u32::from_le_bytes(length.try_into().unwrap()) as usize;

        if buffer.len() < 24 + length {
            // We don't have enough bytes for the rest of the message.
            Err(StreamReaderError::NotEnoughData)?
        }

        let payload = Vec::from(&buffer[24..(24 + length)]);

        // Ugh. Can we somehow make the trait do all of this work for us?
        let payload = if command_bytes == messages::Version::COMMAND {
            if let Some((version, _size)) = messages::Version::from_bytes(&payload) {
                // assert_eq!(size, length);
                MessagePayload::Version(version)
            }
            else {
                MessagePayload::Unknown(payload)
            }
        }
        else if command_bytes == messages::VerAck::COMMAND {
            if let Some((verack, _size)) = messages::VerAck::from_bytes(&payload) {
                // assert_eq!(size, length);
                MessagePayload::VerAck(verack)
            }
            else {
                MessagePayload::Unknown(payload)
            }
        }
        else if command_bytes == messages::Alert::COMMAND {
            if let Some((alert, _size)) = messages::Alert::from_bytes(&payload) {
                // assert_eq!(size, length);
                MessagePayload::Alert(alert)
            }
            else {
                MessagePayload::Unknown(payload)
            }
        }
        else if command_bytes == messages::Addr::COMMAND {
            if let Some((addr, _size)) = messages::Addr::from_bytes(&payload) {
                // assert_eq!(size, length);
                MessagePayload::Addr(addr)
            }
            else {
                MessagePayload::Unknown(payload)
            }
        }
        else if command_bytes == messages::Ping::COMMAND {
            if let Some((ping, _size)) = messages::Ping::from_bytes(&payload) {
                // assert_eq!(size, length);
                MessagePayload::Ping(ping)
            }
            else {
                MessagePayload::Unknown(payload)
            }
        }
        else {
            MessagePayload::Unknown(payload)
        };

        Ok((Message { command, payload }, (24 + length)))
    }
}

impl<T> Stream for StreamReader<T>
where
    T: AsyncRead,
    T: Unpin,
{
    type Item = Result<Message, StreamReaderError>;

    /// The meat of a stream. Called whenever it's time to see if there's another item.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_done {
            return Poll::Ready(None);
        }

        loop {
            // Ugh. We get a "can't borrow two things mutably" error when trying to use our reader
            // to parse our buffer. So instead, we have an optional buffer, and we swap it out
            // before using it, then swap it back in when we need to save it. Looks ugly, but it
            // works.
            assert!(self.buffer.is_some());
            let mut buffer = std::mem::replace(&mut self.buffer, None).unwrap();
            let read_bytes = self.read_bytes.clone();

            match self.parse(&buffer[read_bytes]) {
                Ok((message, parse_size)) => {
                    // Why?
                    if parse_size == 0 {
                        return Poll::Ready(None);
                    }

                    self.read_bytes.start = self.read_bytes.start + parse_size;
                    if self.read_bytes.start == self.read_bytes.end {
                        self.read_bytes = 0..0
                    }

                    self.buffer = Some(buffer);
                    // We have a message! Return it.
                    return Poll::Ready(Some(Ok(message)));
                }
                Err(StreamReaderError::NotEnoughData) => {
                    // We need to read some more data!
                }
                Err(err) => {
                    self.is_done = true;
                    self.buffer = Some(buffer);
                    // We have an error. Return it.
                    return Poll::Ready(Some(Err(err)));
                }
            }

            if buffer.len() - self.read_bytes.end < 4096 {
                buffer.resize(buffer.len() + 4096, 0);
            }

            let read_bytes = self.read_bytes.clone();

            match Pin::new(&mut self.reader).poll_read(cx, &mut buffer[read_bytes.end..]) {
                Poll::Ready(Err(e)) => {
                    println!("err={:?}", e);
                    self.buffer = Some(buffer);
                    // Uh oh. Something happened. Consider the stream done.
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    self.buffer = Some(buffer);
                    // No new data. Tell the thing that wants new data that there's no new data.
                    return Poll::Pending;
                }
                Poll::Ready(Ok(size)) => {
                    self.read_bytes.end = self.read_bytes.end + size;

                    if size == 0 {
                        // This is the end of the stream. There will be no messages.
                        // TODO: Do we care if we have any existing data?
                        self.is_done = true;
                        self.buffer = Some(buffer);
                        // Consider the stream done.
                        return Poll::Ready(None);
                    }

                    // Save off our buffer for the next time around.
                    self.buffer = Some(buffer);
                }
            };
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use async_std::{prelude::*, task, stream::Stream};

    #[test]
    fn test_parse_basic() {
        let buf = [0];
        let mut stream_reader = StreamReader::new(
            Network::Testnet3,
            &buf,
        );

        let message_data = vec![
            0x0b, 0x11, 0x09, 0x07, // Testnet3 header
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x00, 0x00, // version
            0x04, 0x00, 0x00, 0x00, // length
            0x00, 0x00, 0x00, 0x00, // checksum
            0x01, 0x23, 0x45, 0x67, // payload
        ];

        let result = stream_reader.parse(&message_data);
        assert!(result.is_ok(), "Result has data");
        let result = result.unwrap();
        let message = result.0;
        let bytes = result.1;
        // assert_eq!(message.command.as_str(), "version");
        // assert_eq!(message.payload.len(), 4);
        // assert_eq!(&message.payload, &[0x01, 0x23, 0x45, 0x67]);
        assert_eq!(bytes, 28);
    }

    #[test]
    fn test_read_basic() {
        let fut = async {
            let message_data: Vec<u8> = vec![
                0x0b, 0x11, 0x09, 0x07, // Testnet3 header
                0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x00, 0x00, // version
                0x04, 0x00, 0x00, 0x00, // length
                0x00, 0x00, 0x00, 0x00, // checksum
                0x01, 0x23, 0x45, 0x67, // payload
            ];
            let mut stream_reader = StreamReader::new(
                Network::Testnet3,
                &message_data[..],
            );

            let message_1 = stream_reader.next().await;
            let message_2 = stream_reader.next().await;
            assert!(message_1.is_some(), "First result is a message");
            assert_eq!(message_2, None);
        };
        task::block_on(fut)
    }

    #[test]
    fn test_read_two_messages() {
        let fut = async {
            let message_data: Vec<u8> = vec![
                0x0b, 0x11, 0x09, 0x07, // Testnet3 header
                0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x00, 0x00, // version
                0x04, 0x00, 0x00, 0x00, // length
                0x00, 0x00, 0x00, 0x00, // checksum
                0x01, 0x23, 0x45, 0x67, // payload

                0x0b, 0x11, 0x09, 0x07, // Testnet3 header
                0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x00, 0x00, // version
                0x04, 0x00, 0x00, 0x00, // length
                0x00, 0x00, 0x00, 0x00, // checksum
                0x01, 0x23, 0x45, 0x67, // payload
            ];
            let mut stream_reader = StreamReader::new(
                Network::Testnet3,
                &message_data[..],
            );

            let message_1 = stream_reader.next().await;
            let message_2 = stream_reader.next().await;
            let message_3 = stream_reader.next().await;
            assert!(message_1.is_some(), "First result is a message");
            assert!(message_2.is_some(), "Second result is a message");
            assert_eq!(message_3, None);
        };
        task::block_on(fut)
    }
}
