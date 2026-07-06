use std::io::Read;
use std::net::TcpStream;

fn main() {
    let address = "127.0.0.1:7879";

    println!("Connecting to queue sender at {}", address);
    let mut stream = TcpStream::connect(address).expect("Failed to connect to queue sender");

    println!("Connected! Waiting for messages...");
    
    let mut buffer = [0; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                println!("Connection closed");
                break;
            }
            Ok(n) => {
                let message = String::from_utf8_lossy(&buffer[..n]);
                println!("Received: {}", message);
            }
            Err(err) => {
                eprintln!("Failed to receive message: {}", err);
                break;
            }
        }
    }
}
