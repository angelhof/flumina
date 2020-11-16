/*
    A few simple abstractions for distributed communication over a network.
*/

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, SocketAddrV4};
// use std::process::Command;
use std::str;
// use std::string::String;
// use std::thread;

pub fn socket(host: &str, port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(host.parse().unwrap(), port)
}

// Handshake: barrier between two distributed processes
// call handshake(s0, true) on s0 and handshake(s0, false) on s1.
pub fn handshake(s0: SocketAddrV4, listener: bool) {
    match listener {
        true => {
            /* Handshake listener */
            println!("[listener] initializing...");
            let listener = TcpListener::bind(s0).unwrap();
            for stream in listener.incoming() {
                println!("[listener] reading...");
                let mut data = [0 as u8; 50];
                let msg = stream.unwrap().read(&mut data).unwrap();
                println!("[listener] got: {}", msg);
                break;
            }
            println!("[listener] handshake complete");
        }
        false => {
            loop {
                println!("[sender] connecting...");
                if let Ok(mut stream) = TcpStream::connect(s0) {
                    println!("[sender] writing...");
                    stream.write(&[1]).unwrap();
                    break;
                }
            }
            println!("[sender] handshake complete")
        }
    }
}

// Barrier between n distributed processes
// fn barrier()
