//! The agent is the actor that connects to and communicates with a bitcoin node. All of the logic
//! for staying connected and dealing with the node lives here.

use async_std::{
    sync::Arc,
    net::TcpStream,
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
        StreamWriter,
        messages::*,
    },
};
use std::time::SystemTime;
use std::net::SocketAddr;

type Sender<T> = mpsc::UnboundedSender<T>;
type Receiver<T> = mpsc::UnboundedReceiver<T>;

/// The agent for a single connection. In retrospect, this could be functions and not a struct.
pub struct Agent {
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

        let agent = Agent {};
        agent.run(network, stream, broker_handle).await
    }

    /// After connecting the TCP socket, run all of the parts of the agent. 
    async fn run(self, network: Network, stream: TcpStream, broker_handle: BrokerHandle) -> Result<(), std::io::Error> {
        let stream = Arc::new(stream);
        let (shutdown_tx, shutdown_rx) = mpsc::unbounded();
        let (read_agent_tx, read_agent_rx) = mpsc::unbounded();
        let (agent_write_tx, agent_write_rx) = mpsc::unbounded();
        let read_future = self.run_reader(network.clone(), stream.clone(), read_agent_tx, shutdown_rx);
        let write_future = self.run_writer(network.clone(), stream.clone(), agent_write_rx);
        let agent_future = self.run_agent(stream.clone(), broker_handle, read_agent_rx, agent_write_tx, shutdown_tx);

        let future = future::try_join!(read_future, write_future, agent_future);

        future.await?;

        Ok(())
    }

    /// Run the logic portion of the agent.
    async fn run_agent(&self, stream: Arc<TcpStream>, mut broker_handle: BrokerHandle, mut receiver: Receiver<Message>, mut sender: Sender<Message>, _shutdown: Sender<Void>) -> Result<(), std::io::Error> {

        // The first thing that needs to be done is to send a "version" command to the node. Get
        // everything we need and construct the message.
        let timestamp: i64 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as i64;
        let peer_addr = stream.peer_addr()?;
        // All IP addresses in the messages are IPv6, so make it happen.
        let peer_ip = match peer_addr.ip() {
            std::net::IpAddr::V4(a) => a.to_ipv6_mapped(),
            std::net::IpAddr::V6(a) => a,
        };
        let local_addr = stream.local_addr()?;
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

        // Send the message!
        sender.send(message).await.unwrap();

        let mut did_connect = false;

        // Receive all messages coming in, and act on them.
        while let Some(message) = receiver.next().await {
            match message.payload {
                MessagePayload::Version(version) => { 
                    did_connect = true;
                    // Let the broker know we connected.
                    broker_handle.connected(version.user_agent, version.version).await;
                    // Let the node know we like it's version. (I guess we always like the
                    // version.)
                    sender.send(Message::verack()).await.unwrap();
                }
                MessagePayload::VerAck(_) => {
                    // Once we're fully connected, ask for all of the addresses that the node knows
                    // about.
                    sender.send(Message::getaddr()).await.unwrap();
                }
                MessagePayload::Ping(ping) => {
                    // Whenever we get a ping, send a pong.
                    sender.send(Message::pong(ping.nonce)).await.unwrap();
                }
                MessagePayload::Addr(addr) => {
                    // Here's some addresses! Tell the broker about them.
                    let addresses = addr.addresses.into_iter().map(|(_timestamp, address)| {
                        std::net::SocketAddr::new(address.ip.into(), address.port)
                    }).collect();
                    broker_handle.new_addresses(addresses).await;
                }
                _ => {},
            }
        }

        // Tell the broker that we disconnected.
        if did_connect {
            broker_handle.disconnected().await;
        }

        Ok(())
    }

    /// Run the stream reader portion of the agent, and forward the messages to the logic portion.
    async fn run_reader(&self, network: Network, stream: Arc<TcpStream>, mut sender: Sender<Message>, mut shutdown: Receiver<Void>) -> Result<(), std::io::Error> {
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
    async fn run_writer(&self, network: Network, stream: Arc<TcpStream>, mut receiver: Receiver<Message>) -> Result<(), std::io::Error> {
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
