use super::{Network, Message, component::*};
use futures::io::AsyncWrite;
use futures::io::AsyncWriteExt;
use sha2::{Digest, Sha256};

/// Write messages to a stream
///
/// This should probably implement Sink, but we don't need that abstraction yet at this point.
pub struct StreamWriter<T> {
    network: Network,
    stream: T,
}

impl<T> StreamWriter<T>
where 
    T: AsyncWrite,
    T: Unpin,
{
    /// Create a new stream writer
    pub fn new(network: Network, stream: T) -> Self {
        StreamWriter { network, stream }
    }

    /// Write a message to the TCP connection
    pub async fn send_message(&mut self, message: Message) -> Result<(), std::io::Error> {
        let mut full_bytes = Vec::new();
        let mut message_bytes = Vec::new();

        message.payload.to_bytes(&mut message_bytes);

        self.network.to_bytes(&mut full_bytes);
        std::io::Write::write(&mut full_bytes, &message.command.0).unwrap();
        (message_bytes.len() as u32).to_bytes(&mut full_bytes);

        let mut sha = Sha256::new();
        sha.input(&message_bytes[..]);
        let output = sha.result_reset();
        sha.input(&output);
        let output = sha.result();
        std::io::Write::write(&mut full_bytes, &output[0..4]).unwrap();

        std::io::Write::write(&mut full_bytes, &message_bytes[..]).unwrap();

        self.stream.write_all(&full_bytes[..]).await?;

        Ok(())
    }
}
