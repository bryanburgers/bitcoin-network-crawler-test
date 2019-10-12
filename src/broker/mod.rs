//! The broker is the actor that keeps track of which IP addresses we've connected to, which ones
//! we've seen, tells the main app to connect to new addresses, etc.
//!
//! We communicate to the broker through channels, so that we don't have to do weird shared memory
//! hacks. In a way, this is *almost* like go-routines, though I didn't plan it that way. I guess
//! it's more like an actor model.


use async_std::prelude::*;
use futures_channel::mpsc;
use futures_util::sink::SinkExt;
use std::net::SocketAddr;
use std::collections::HashSet;

/// The data broker for the entire application.
pub struct Broker {
    /// The channel on which we will receive events from agents
    receiver: mpsc::UnboundedReceiver<BrokerMessage>,
    /// The channel on which we will send new addresses to connect to
    spawner: mpsc::UnboundedSender<SocketAddr>,
}

impl Broker {
    /// Create a new one broker, and a new "BrokerHandleGenerator" (terrible name) that can hand
    /// out handles to the broker that was just created.
    pub fn new(spawner: mpsc::UnboundedSender<SocketAddr>) -> (Broker, BrokerHandleGenerator) {
        let (sender, receiver) = mpsc::unbounded();
        let broker = Broker {
            receiver,
            spawner,
        };
        let handle_generator = BrokerHandleGenerator {
            sender,
        };
        (broker, handle_generator)
    }

    /// Run the broker as a future.
    pub async fn run(mut self) -> Result<(), std::io::Error> {
        // All of the IP addresses that we've seen!
        let mut seen = HashSet::new();
        // How many times an agent tried to connect and failed
        let mut failed_connections = 0;
        // How many connections we're still waiting on
        let mut limbo = 0;
        // How many times a connection has succeeded
        let mut connected = 0;
        // How many times a successful connection  has finished
        let mut disconnected = 0;

        while let Some(message) = self.receiver.next().await {
            match message {
                BrokerMessage::Initializing { .. } => {
                    // We could do more in a lot of these. We aren't right now. For now, keep track
                    // that we are starting a connection. This is in "limbo", i.e. we don't know if
                    // it's good or not.
                    limbo += 1;
                }
                BrokerMessage::FailedConnection { .. } => {
                    // Boo, it failed.
                    failed_connections += 1;
                    // But we know it failed. It's no longer in limbo.
                    limbo -= 1;
                }
                BrokerMessage::Connected { .. } => {
                    // Yay!
                    connected += 1;
                    limbo -= 1;
                }
                BrokerMessage::NewAddresses { addresses, .. } => {
                    // Ooh goody, an agent found out about some more addresses.
                    for address in addresses {
                        // Keep track of the fact that we've seen them.
                        if seen.insert(address) {
                            // And if they're new, try to connect to them.
                            self.spawner.send(address).await.unwrap()
                        }
                    }
                }
                BrokerMessage::Disconnected { .. } => {
                    // All good things come to an end.
                    disconnected += 1;
                }
            }

            // TODO: Can we use a timeout future to somehow display this on a regular interval,
            // instead of anytime an event happens?
            println!("connected={}, disconnected={}, current={}, limbo={}, failed={}, seen={}", connected, disconnected, connected - disconnected, limbo, failed_connections, seen.len());
        }

        Ok(())
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
