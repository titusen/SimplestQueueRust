use std::env;
use std::io::Write;
use std::net::{Shutdown, TcpStream};
use std::thread::sleep;
use rand::prelude::*;

fn main() {
    let address = "127.0.0.1:7878";
    let args: Vec<String> = env::args().collect();
    let message = args.get(1).map(|s| s.as_str()).unwrap_or("Hello from sender!");
    let mut counter = 0;


    println!("Connecting to receiver at {}", address);
    let mut stream = TcpStream::connect(address).expect("Failed to connect to receiver");
    stream.set_nodelay(true).expect("set_nodelay call failed");
    while counter < 50 {
        let message = format!("{} - ggggggggggggggggggggggggggggggMessage {}\n", message, counter);
        let _ = stream
            .write_all(message.as_bytes());
        stream.flush().expect("flush call failed");
        println!("Sent message: {}", message);
        counter += 1;
        let mut rng = rand::rng();
        let sleep_ms = rng.random::<u64>() % 100;
        sleep(std::time::Duration::from_millis(400 + sleep_ms));
    }
    stream.shutdown(Shutdown::Write)
        .expect("Failed to close write half");

}
