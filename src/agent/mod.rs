//! The agent is the actor that connects to and communicates with a bitcoin node. All of the logic
//! for staying connected and dealing with the node lives here.

use async_std::{
    sync::Arc,
    net::TcpStream,
    stream,
    future,
    prelude::*,
};
use futures_channel::mpsc;
use futures_util::{
    sink::SinkExt,
    select,
    FutureExt
};
use crate::{
    broker::BrokerHandle,
    bitcoin_protocol::{
        self,
        Message,
        MessagePayload,
        Network,
        StreamReader,
        StreamReaderError,
        StreamWriter,
        messages::*,
    },
};
use std::time::{SystemTime, Duration};
use std::net::SocketAddr;

type Sender<T> = mpsc::UnboundedSender<T>;
type Receiver<T> = mpsc::UnboundedReceiver<T>;

/// The agent for a single connection.
pub struct Agent {
    /// The handle used to send updates to the broker.
    broker_handle: BrokerHandle,
    /// The TCP stream that we're connected to.
    stream: Arc<TcpStream>,
    /// A stream of messages, coming from the read side of the stream.
    read_channel: Receiver<Message>,
    /// A stream of messages, sending to the write side of the stream.
    write_channel: Sender<Message>,
    /// A channel that we keep around. Once it gets dropped, it sends a synchronization message
    /// that it was dropped. This is used to shut down the reader if it is still alive.
    _shutdown: Sender<Void>
}

/// A type that is uninhabitable. Used for the shutdown channel to only send the "end of stream"
/// synchrnoization event.
enum Void {}

impl Agent {
    /// Connect to a node!
    pub async fn connect(network: Network, address: SocketAddr, mut broker_handle: BrokerHandle) -> Result<(), std::io::Error> {
        broker_handle.initializing().await;
        let stream = match TcpStream::connect(address).await {
            Ok(stream) => stream,
            Err(err) => {
                broker_handle.failed_connection().await;
                Err(err)?
            }
        };

        Self::run(network, stream, broker_handle).await
    }

    /// After connecting the TCP socket, run all of the parts of the agent. 
    async fn run(network: Network, stream: TcpStream, broker_handle: BrokerHandle) -> Result<(), std::io::Error> {
        let stream = Arc::new(stream);

        // We break up the work into three parts: the reader reads messages from the stream and
        // sends them along on a channel. The writer listens on a channel and writes messages back
        // to the stream. The core work is provided by the third part, which takes messages from
        // the reader, decides what to do, and sends messages to the writer.

        // So, we need some channels to communicate between the three parts.
        let (shutdown_tx, shutdown_rx) = mpsc::unbounded();
        let (read_agent_tx, read_agent_rx) = mpsc::unbounded();
        let (agent_write_tx, agent_write_rx) = mpsc::unbounded();

        // Kick off the reader.
        let read_future = Self::run_reader(network.clone(), stream.clone(), read_agent_tx, shutdown_rx);
        // Kick off the writer.
        let write_future = Self::run_writer(network.clone(), stream.clone(), agent_write_rx);

        // Build the agent and kick it off.
        let agent = Agent {
            broker_handle,
            stream: stream.clone(),
            read_channel: read_agent_rx,
            write_channel: agent_write_tx,
            _shutdown: shutdown_tx,
        };
        let agent_future = agent.run_agent();

        let future = future::try_join!(read_future, write_future, agent_future);

        future.await?;

        Ok(())
    }

    /// Run the logic portion of the agent.
    async fn run_agent(mut self) -> Result<(), std::io::Error> {
        // We're connected to TCP! Now try to do all of the bitcoin protocol work.
        self.broker_handle.negotiating().await;

        // The first thing that needs to be done is to send a "version" command to the node. Get
        // everything we need and construct the message.
        let timestamp: i64 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as i64;
        let peer_addr = self.stream.peer_addr()?;
        // All IP addresses in the messages are IPv6, so make it happen.
        let peer_ip = match peer_addr.ip() {
            std::net::IpAddr::V4(a) => a.to_ipv6_mapped(),
            std::net::IpAddr::V6(a) => a,
        };
        let local_addr = self.stream.local_addr()?;
        let local_ip = match local_addr.ip() {
            std::net::IpAddr::V4(a) => a.to_ipv6_mapped(),
            std::net::IpAddr::V6(a) => a,
        };

        // Construct the message.
        let message = Message::version(Version {
            // protocol version. This seems to be a popular one.
            version: 70015,
            services: 1,
            timestamp,
            nonce: 0,
            addr_recv: bitcoin_protocol::component::Address {
                services: 1,
                ip: peer_ip,
                port: peer_addr.port(),
            },
            addr_from: bitcoin_protocol::component::Address {
                services: 1,
                ip: local_ip,
                port: local_addr.port(),
            },
            user_agent: "/bitcoin-network-crawler-test/".into(),
            start_height: 0,
            relay: false,
        });

        // Send the version message!
        self.write_channel.send(message).await.unwrap();

        // Set up a timer so that every minute, we send another getaddr. This way we get the new
        // nodes from this node.
        let mut getaddr_interval = stream::interval(Duration::from_millis(60 * 1000));

        loop {
            select! {
                message = self.read_channel.next().fuse() => match message {
                    Some(message) => self.handle_message(message).await,
                    // End-of-stream!
                    None => break,
                },
                _ = getaddr_interval.next().fuse() => self.resend_getaddr().await,
            }
        }

        // Tell the broker that we disconnected.
        self.broker_handle.disconnected().await;

        Ok(())
    }

    /// Handle a message from the other end.
    async fn handle_message(&mut self, message: Message) {
        match message.payload {
            MessagePayload::Version(version) => { 
                // Let the broker know we connected.
                self.broker_handle.connected(version.user_agent, version.version).await;
                // Let the node know we like it's version. (I guess we always like the
                // version.)
                self.write_channel.send(Message::verack()).await.unwrap();
            }
            MessagePayload::VerAck(_) => {
                // Once we're fully connected, ask for all of the addresses that the node knows
                // about.
                self.write_channel.send(Message::getaddr()).await.unwrap();
            }
            MessagePayload::Ping(ping) => {
                // Whenever we get a ping, send a pong.
                self.write_channel.send(Message::pong(ping.nonce)).await.unwrap();
            }
            MessagePayload::Addr(addr) => {
                // Here's some addresses! Tell the broker about them.
                let addresses = addr.addresses.into_iter().map(|(_timestamp, address)| {
                    std::net::SocketAddr::new(address.ip.into(), address.port)
                }).collect();
                self.broker_handle.new_addresses(addresses).await;
            }
            _ => {},
        }
    }

    /// Send the getaddr again, because it's been a while since we've done so.
    async fn resend_getaddr(&mut self) {
        self.write_channel.send(Message::getaddr()).await.unwrap();
    }

    /// Run the stream reader portion of the agent, and forward the messages to the logic portion.
    async fn run_reader(network: Network, stream: Arc<TcpStream>, mut sender: Sender<Message>, mut shutdown: Receiver<Void>) -> Result<(), std::io::Error> {
        let stream = &*stream;
        let mut stream_reader = StreamReader::new(network, stream);

        loop {
            select! {
                // We received a message from the reader
                message = stream_reader.next().fuse() => match message {
                    Some(message) => match message {
                        Ok(message) => {
                            // println!("↓ {}", message.command.as_str());
                            sender.send(message).await.unwrap()
                        }
                        Err(StreamReaderError::ConnectionError(_)) => {
                            // An error occurred with the connection. This is pretty much the same
                            // as the connecting closing, so we're done.
                            break;
                        },
                        Err(err) => {
                            println!("Agent::run_reader got an error! {:?}", err);
                        }
                    }
                    None => break,
                },
                // The logic portion of the agent wants to shutdown.
                void = shutdown.next().fuse() => match void {
                    Some(void) => match void {},
                    None => break
                },
            }
        }

        Ok(())
    }

    /// Run the stream writer portion of the agent. The logic portion forwards messages here.
    async fn run_writer(network: Network, stream: Arc<TcpStream>, mut receiver: Receiver<Message>) -> Result<(), std::io::Error> {
        // WHAT!? The async-std book did this. I'm not sure if they had some invarients they were
        // keeping to make this a valid move.
        let stream = &*stream;
        let mut stream_writer = StreamWriter::new(network, stream);

        while let Some(message) = receiver.next().await {
            // println!("↑ {}", message.command.as_str());
            stream_writer.send_message(message).await?;
        }

        Ok(())
    }
}
