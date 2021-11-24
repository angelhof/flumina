/*
    A few simple abstractions for distributed communication over a network.
*/

use super::util::sleep_for_secs;

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::str;

fn parse_host(host: &str) -> Ipv4Addr {
    if host.to_lowercase() == "localhost" {
        Ipv4Addr::LOCALHOST
    } else {
        host.parse().unwrap_or_else(|err| {
            panic!("Unable to parse '{}' as an IPV4 address: {}", host, err)
        })
    }
}

fn socket(host: &str, port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(parse_host(host), port)
}

// Handshake: barrier between two distributed processes
// call handshake(s0, true) on s0 and handshake(s0, false) on s1.
const HANDSHAKE_SLEEP: u64 = 2;
pub fn handshake(s0: SocketAddrV4, listener: bool) {
    if listener {
        /* Handshake listener */
        // println!("[listener] initializing...");
        let listener = TcpListener::bind(s0).unwrap_or_else(|err| {
            panic!("Failed to start TCP connection at {:?}: {}", s0, err);
        });
        // println!("[listener] waiting...");
        let stream = listener.incoming().next().unwrap_or_else(|| {
            panic!("Failed to get stream using TCP (got None) at {:?}", s0);
        });
        // println!("[listener] reading...");
        let mut data = [0 as u8; 50];
        let _msg = stream
            .unwrap_or_else(|err| {
                panic!("Listener failed to get message from stream: {}", err);
            })
            .read(&mut data)
            .unwrap_or_else(|err| {
                panic!("Listener failed to read message from stream: {}", err);
            });
    // println!("[listener] got: {}", msg);
    // println!("[listener] handshake complete");
    } else {
        /* Handshake sender */
        loop {
            // println!("[sender] waiting...");
            if let Ok(mut stream) = TcpStream::connect(s0) {
                // println!("[sender] writing...");
                stream.write_all(&[1]).unwrap();
                break;
            } else {
                // println!("[sender] sleeping for {}s", HANDSHAKE_SLEEP);
                sleep_for_secs(HANDSHAKE_SLEEP);
            }
        }
        // println!("[sender] handshake complete")
    }
}

// Barrier between n distributed processes
// uses host 'host' on process 0
// precondition: the range of ports [port + 1, port + 2 * num_nodes) is unique
// for each call
// uses port port + i to communicate between host (index 0) and index i
pub fn barrier(host: &str, num_nodes: u64, this_node: u64, start_port: u16) {
    assert!(this_node < num_nodes);
    for phase in 0..2 {
        // println!(
        //     "[node {}/{}] barrier phase {}",
        //     this_node, num_nodes, phase + 1
        // );
        if this_node == 0 {
            /* Listener */
            for i in 1..num_nodes {
                // println!(
                //     "[node {}/{}] listening for handshake from {}",
                //     this_node, num_nodes, i
                // );
                let socket0 = socket(
                    host,
                    start_port + (num_nodes as u16) * phase + (i as u16),
                );
                handshake(socket0, true);
            }
        } else {
            /* Sender  */
            // println!("[node {}/{}] sending handshake", this_node, num_nodes);
            let socket0 = socket(
                host,
                start_port + (num_nodes as u16) * phase + (this_node as u16),
            );
            handshake(socket0, false);
        }
    }
    // println!("[node {}/{}] barrier complete", this_node, num_nodes);
}
