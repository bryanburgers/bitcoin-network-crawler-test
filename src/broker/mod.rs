//! The broker is the actor that keeps track of which IP addresses we've connected to, which ones
//! we've seen, tells the main app to connect to new addresses, etc.
//!
//! We communicate to the broker through channels, so that we don't have to do weird shared memory
//! hacks. In a way, this is *almost* like go-routines, though I didn't plan it that way. I guess
//! it's more like an actor model.

use async_std::{
    prelude::*,
    stream,
};
use chrono::{
    DateTime,
    Utc,
    Local,
};
use futures_channel::mpsc;
use futures_util::{
    FutureExt,
    select,
    sink::SinkExt,
};
use std::time::Duration;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::net::IpAddr;

/// The data broker for the entire application.
pub struct Broker {
    /// The channel on which we will receive events from agents
    receiver: mpsc::UnboundedReceiver<BrokerMessage>,
    /// The channel on which we will send new addresses to connect to
    spawner: mpsc::UnboundedSender<SocketAddr>,
    /// All of the IP addresses that we've seen!
    addresses: HashMap<IpAddr, AddressInfo>,
}

impl Broker {
    /// Create a new one broker, and a new "BrokerHandleGenerator" (terrible name) that can hand
    /// out handles to the broker that was just created.
    pub fn new(spawner: mpsc::UnboundedSender<SocketAddr>) -> (Broker, BrokerHandleGenerator) {
        let (sender, receiver) = mpsc::unbounded();
        let broker = Broker {
            receiver,
            spawner,
            addresses: HashMap::new(),
        };
        let handle_generator = BrokerHandleGenerator {
            sender,
        };
        (broker, handle_generator)
    }

    /// Run the broker as a future.
    pub async fn run(mut self) -> Result<(), std::io::Error> {
        let mut output_timer = stream::interval(Duration::from_millis(10 * 1000));
        let mut detailed_output_timer = stream::interval(Duration::from_millis(2 * 60 * 1000));

        loop {
            select! {
                message = self.receiver.next().fuse() => match message {
                    Some(message) => self.handle_message(message).await,
                    None => break,
                },
                _ = output_timer.next().fuse() => self.print_stats().await,
                _ = detailed_output_timer.next().fuse() => self.print_detailed_stats().await,
            }
        }

        Ok(())
    }

    async fn handle_message(&mut self, message: BrokerMessage) -> () {
        match message {
            BrokerMessage::Initializing { address } => {
                // An agent is trying to connect to a remote node.
                let info = self.addresses.entry(address.ip()).or_default();
                info.current_state = AddressInfoConnectionState::Connecting;
            }
            BrokerMessage::FailedConnection { address } => {
                // Boo, it failed.
                let info = self.addresses.entry(address.ip()).or_default();
                info.current_state = AddressInfoConnectionState::Disconnected {
                    next_attempt: None,
                };
            }
            BrokerMessage::Negotiating { address } => {
                // Almost there: the agent established a TCP connection. Now it's trying to do the
                // bitcoin protocol's version/verack dance.
                let info = self.addresses.entry(address.ip()).or_default();
                info.current_state = AddressInfoConnectionState::Negotiating;
            }
            BrokerMessage::Connected { address, user_agent, version } => {
                // Yay! We successfully connected to the node!
                let info = self.addresses.entry(address.ip()).or_default();
                info.current_state = AddressInfoConnectionState::Connected {
                    _since: Utc::now(),
                };
                info.historical_state = AddressInfoHistoricalState::PreviouslyConnected {
                    info: NodeInfo {
                        user_agent,
                        protocol_version: version,
                    },
                };
            }
            BrokerMessage::NewAddresses { addresses, .. } => {
                // Ooh goody, an agent found out about some more addresses.
                for address in addresses {
                    let info = self.addresses.entry(address.ip()).or_default();
                    let now = Utc::now();
                    match info.current_state {
                        AddressInfoConnectionState::Disconnected { next_attempt: Some(next_attempt) } if next_attempt <= now => {
                            // This one appears to be new! Let's connect it.
                            info.current_state = AddressInfoConnectionState::Spawning;
                            self.spawner.send(address).await.unwrap()
                        },
                        _ => {},
                    }
                }
            }
            BrokerMessage::Disconnected { address } => {
                // All good things come to an end.
                let info = self.addresses.entry(address.ip()).or_default();
                info.current_state = AddressInfoConnectionState::Disconnected {
                    // TODO: We should reattempt this, because it was good at one point. Currently
                    // we don't.
                    next_attempt: None,
                };
            }
        }
    }

    /// Print out the basic status of the current state of the application.
    ///
    /// This prints out things like how many nodes we're connected to, how many we're trying to
    /// connect to, how many we've seen, etc.
    async fn print_stats(&mut self) {
        #[derive(Default)]
        struct LocalData {
            /// The total number we've seen
            seen: usize,
            /// The number that are trying to connect
            connecting_tcp: usize,
            /// The number that connected to TCP and are still waiting on a verack
            connecting_bitcoin: usize,
            /// The number that have successfully connected at some point
            connected: usize,
            /// The number that are actively connected
            currently_connected: usize,
            /// The number that we've seen but have never successfully connected to.
            failed: usize,
        };

        let data: LocalData = self.addresses.values().fold(Default::default(), |mut local_data, item| {
            local_data.seen += 1;

            match item.current_state {
                AddressInfoConnectionState::Disconnected { .. } => {},
                AddressInfoConnectionState::Spawning => {
                    local_data.connecting_tcp += 1;
                },
                AddressInfoConnectionState::Connecting => {
                    local_data.connecting_tcp += 1;
                },
                AddressInfoConnectionState::Negotiating => {
                    local_data.connecting_bitcoin += 1;
                },
                AddressInfoConnectionState::Connected { .. } => {
                    local_data.currently_connected += 1;
                }
            };

            match item.historical_state {
                AddressInfoHistoricalState::NeverConnected => {
                    local_data.failed += 1;
                },
                AddressInfoHistoricalState::PreviouslyConnected { .. } => {
                    local_data.connected += 1;
                },
            };

            local_data
        });

        println!("{}: total connected={}, currently connected={}, currently connecting={} (tcp={}, bitcoin={}), total seen={}",
                 Local::now().format("%I:%M:%S %P"),
                 data.connected,
                 data.currently_connected,
                 data.connecting_tcp + data.connecting_bitcoin,
                 data.connecting_tcp,
                 data.connecting_bitcoin,
                 data.seen);
    }

    /// Print out a detailed status of the state of the application.
    ///
    /// Essentially, print out user agent and protocol version histograms.
    async fn print_detailed_stats(&self) {
        let mut user_agents = BTreeMap::new();
        let mut versions = BTreeMap::new();

        for value in self.addresses.values() {
            match value.historical_state {
                AddressInfoHistoricalState::PreviouslyConnected { ref info } => {
                    let entry = user_agents.entry(&info.user_agent[..]).or_insert(0_usize);
                    *entry += 1;
                    let entry = versions.entry(info.protocol_version).or_insert(0_usize);
                    *entry += 1;
                },
                _ => {},
            }
        }

        println!("User agents:");
        for (user_agent, count) in user_agents.iter() {
            println!("  {}: {}", user_agent, count);
        }

        println!("Protocol versions:");
        for (version, count) in versions.iter() {
            println!("  {}: {}", version, count);
        }
    }
}

/// A thing that passes out handles to the broker. We can't make the broker itself do this, because
/// it consumes itself when it runs. So we this thing do it.
///
/// It basically just holds the sender, so we can clone it and hand it out.
pub struct BrokerHandleGenerator {
    /// The channel on which to send message to the broker.
    sender: mpsc::UnboundedSender<BrokerMessage>,
}

impl BrokerHandleGenerator {
    /// Get a handle to the broker.
    pub fn get_handle(&self, address: SocketAddr) -> BrokerHandle {
        BrokerHandle { address, sender: self.sender.clone() }
    }
}

/// The messages that get sent to the broker. This is an implementation detail and isn't seen by
/// the agent.
#[allow(dead_code)]
enum BrokerMessage {
    /// We're going to try to start a connection.
    Initializing {
        address: SocketAddr,
    },
    /// Failed to start the connection.
    FailedConnection {
        address: SocketAddr,
    },
    /// We make a TCP connection, and now we're doing the bitcoin protocol connection.
    Negotiating {
        address: SocketAddr,
    },
    /// Succeeded.
    Connected {
        address: SocketAddr,
        user_agent: String,
        version: i32,
    },
    /// The connection found new addresses.
    NewAddresses {
        address: SocketAddr,
        addresses: Vec<SocketAddr>,
    },
    /// Disconnected.
    Disconnected {
        address: SocketAddr,
    },
}

/// An object that each agent gets, to communicate with the broker.
#[derive(Clone)]
pub struct BrokerHandle {
    /// The address that the agent was given to connect to.
    address: SocketAddr,
    /// The channel to send messages on.
    sender: mpsc::UnboundedSender<BrokerMessage>,
}

impl BrokerHandle {
    /// Send the Initializing message
    pub async fn initializing(&mut self) {
        self.sender.send(BrokerMessage::Initializing {
            address: self.address,
        }).await.unwrap();
    }

    /// Send the FailedConnection message
    pub async fn failed_connection(&mut self) {
        self.sender.send(BrokerMessage::FailedConnection {
            address: self.address,
        }).await.unwrap();
    }

    /// Send the Negotiating message
    pub async fn negotiating(&mut self) {
        self.sender.send(BrokerMessage::Negotiating {
            address: self.address,
        }).await.unwrap();
    }

    /// Send the Connected message
    pub async fn connected(&mut self, user_agent: String, version: i32) {
        self.sender.send(BrokerMessage::Connected {
            address: self.address,
            user_agent,
            version,
        }).await.unwrap();
    }

    /// Send the NewAddresses message
    pub async fn new_addresses(&mut self, addresses: Vec<SocketAddr>) {
        self.sender.send(BrokerMessage::NewAddresses {
            address: self.address,
            addresses,
        }).await.unwrap()
    }

    /// Send the Disconnected message
    pub async fn disconnected(&mut self) {
        self.sender.send(BrokerMessage::Disconnected {
            address: self.address,
        }).await.unwrap()
    }
}

/// Stored information about a single address.
struct AddressInfo {
    /// The state of the current connection attempt (if any).
    current_state: AddressInfoConnectionState,
    /// Information about whether we have ever successfully connected to this node.
    historical_state: AddressInfoHistoricalState,
}

impl Default for AddressInfo {
    fn default() -> Self{
        AddressInfo {
            current_state: AddressInfoConnectionState::Disconnected { next_attempt: Some(Utc::now()) },
            historical_state: AddressInfoHistoricalState::NeverConnected,
        }
    }
}

/// If we have successfully connected to a node, we know some information about it. This stores
/// that information.
struct NodeInfo {
    /// The user agent of the node.
    user_agent: String,
    /// Which version of the bitcoin protocol the node is advertising.
    protocol_version: i32,
}

/// The state of any current attempts to connect to the node.
enum AddressInfoConnectionState {
    /// The node is currently disconnected. We can attempt to connect to it at next_attempt.
    ///
    /// Note: currently we're not using next_attempt, but the idea is that we could ocassionally
    /// check all disconnected connections and try to reconnect them at a later time.
    Disconnected { next_attempt: Option<DateTime<Utc>> },
    /// The broker has told the spawner to spawn an agent for this address, but we haven't yet
    /// heard back from a spawned agent.
    Spawning,
    /// The agent got back to us. It's going to start connecting to the address!
    Connecting,
    /// The agent got a TCP connection, and is now doing the version/verack handshake.
    Negotiating,
    /// The version/verack handshake is done! We are fully connected to a node!
    Connected { _since: DateTime<Utc> },
}

/// Information about whether this address has every successfully been connected.
enum AddressInfoHistoricalState {
    /// We've never successfully connected to this address. That's a bummer, but it happens.
    NeverConnected,
    /// We either currently are or have been connected to this address. And because we've been
    /// connected, we know information about the node on the other end!
    ///
    /// To determine whether we are currently connected or not, check the connection state. That's
    /// not the responsibility of this field to know.
    PreviouslyConnected { info: NodeInfo },
}
