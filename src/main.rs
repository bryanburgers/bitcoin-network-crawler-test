//! The early stages of a test program I'm using to get up-to-speed in Rust's
//! `async`/`await`.
//!
//! The program connects to as many bitcoin nodes as possible, stays connected by
//! sending `pong` messages to the `ping`s, and asks each node for any other nodes
//! it knows about. The program then connects to those nodes and does the same
//! thing.
//!
//! This is in a bit of a rough state right now, but throwing it up on GitHub
//! anyway, in case it's useful to anybody else.

use async_std::{
    net::{ToSocketAddrs},
    prelude::*,
    task,
};
use clap::{App, Arg};
use std::net::SocketAddr;

mod agent;
mod bitcoin_protocol;
mod broker;

use agent::Agent;
use broker::BrokerHandle;

fn main() -> Result<(), std::io::Error> {
    let matches = App::new("bitcoin-network-crawler-test")
        .version("1.0")
        .author("Bryan Burgers <bryan@burgers.io>")
        .about("Tries to crawl the bitcoin network")
        .arg(Arg::with_name("address").required(true))
        .arg(Arg::with_name("network")
             .short("n")
             .long("network")
             .takes_value(true)
             .possible_values(&["main", "testnet", "testnet3", "namecoin"])
             .default_value("testnet3"))
        .get_matches();

    // # testnet
    // cargo +beta run testnet-seed.bitcoin.jonasschnelli.ch:18333 --network testnet3
    //
    // # main
    // cargo +beta run seed.bitcoin.sprovoost.nl:8333 --network main
    let seed_address = matches.value_of("address").unwrap();
    let network = match matches.value_of("network").unwrap() {
        "main" => bitcoin_protocol::Network::Main,
        "testnet" => bitcoin_protocol::Network::Testnet,
        "testnet3" => bitcoin_protocol::Network::Testnet3,
        "namecoin" => bitcoin_protocol::Network::Namecoin,
        unknown => {
            panic!("Unknown network '{}'", unknown);
        }
    };

    let fut = run(network, seed_address);
    task::block_on(fut)
}

/// Communicate with a single bitcoin node, sending the broker information about the connection
/// when possible.
async fn run_one(network: bitcoin_protocol::Network, address: SocketAddr, handle: BrokerHandle) -> Result<(), std::io::Error> {
    Agent::connect(network, address, handle).await
}

/// Communicate with as many bitcoin nodes as possible.
async fn run(network: bitcoin_protocol::Network, address: impl ToSocketAddrs) -> Result<(), std::io::Error> {
    // A channel we use for the broker to tell us when to spawn new connections.
    let (spawner_send, mut spawner_receive) = futures_channel::mpsc::unbounded();

    // Create a new broker and run it. The handle_generator (bad name, I should pick a new one)
    // creates handles and passes them out to the agents.
    let (broker, handle_generator) = broker::Broker::new(spawner_send);
    task::spawn(broker.run());

    // Turn that one seed address into a bunch of IP addresses!
    let addrs = address.to_socket_addrs().await?;

    // And connect to each of them!
    for addr in addrs {
        let broker_handle = handle_generator.get_handle(addr);
        task::spawn(run_one(network.clone(), addr, broker_handle));
    }

    // Listen for messages on the spawner channel and spawn connections.
    while let Some(mut address) = spawner_receive.next().await {
        // It turns out that bitcoin nodes know about the ip/port pair that connected to them. But
        // that's not a listening address! So try to connect to the same address, but at the
        // typical bitcoin port, to see if there's a node there.
        address.set_port(network.default_port());
        let broker_handle = handle_generator.get_handle(address.clone());
        task::spawn(run_one(network.clone(), address.clone(), broker_handle));
    }

    Ok(())
}
