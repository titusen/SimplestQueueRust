use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc};
use SimplestQueueRust::BlockingQueue;
use tokio;

#[tokio::main]
async fn main() {
    let address_recv = "127.0.0.1:7878";
    let address_send = "127.0.0.1:7879";

    let queue = Arc::new(BlockingQueue::<String>::new());

    let recv_queue = Arc::clone(&queue);
    let recv_thread = tokio::spawn(async move { run_receiver(address_recv, recv_queue).await });

    let send_queue = Arc::clone(&queue);
    let send_thread = tokio::spawn(async move { run_sender(address_send, send_queue).await });
    recv_thread.await.expect("Receiver thread panicked");
    send_thread.await.expect("Sender thread panicked");//.expect("Sender thread panicked");
}

async fn run_sender(address: &str, queue: Arc<BlockingQueue<String>>) {
    println!("Starting sender endpoint on {}", address);
    let listener = TcpListener::bind(address).expect("Failed to bind sender endpoint");

    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                let queue = Arc::clone(&queue);
                stream.set_nodelay(true).expect("set_nodelay call failed");
                println!("Received connection send to {}", stream.peer_addr().unwrap());
                tokio::spawn(async move { handle_sending_client(stream, queue) });
            }
            Err(err) => eprintln!("Connection failed: {}", err),
        }
    }
}

async fn run_receiver(address: &str, queue: Arc<BlockingQueue<String>>) {
    println!("Starting receiver endpoint on {}", address);
    let listener = TcpListener::bind(address).expect("Failed to bind receiver endpoint");

    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                let queue = Arc::clone(&queue);
                println!("Received connection receive from {}", stream.peer_addr().unwrap());
                let _ = tokio::spawn(async move { 
                    handle_receiving_client(stream, queue); });

            }
            Err(err) => eprintln!("Connection failed: {}", err),
        }
    }
}

fn handle_receiving_client(mut stream: TcpStream, queue: Arc<BlockingQueue<String>>) {
    println!("Receive client thread started");
    loop {
        let mut buffer = Vec::new();
        match stream.read_to_end(&mut buffer) {
            Ok(n) => {
                if n > 0 {
                    let payload = String::from_utf8_lossy(&buffer);
                    println!("Received {} bytes", buffer.len());
                    queue.en_q(payload.into());
                }

            }
            Err(err) => {
                eprintln!("Failed to read from client: {}", err);
                break;
            }
        }
    }
}

fn handle_sending_client(mut stream: TcpStream, queue: Arc<BlockingQueue<String>>) {
    println!("Sending client thread started");
    loop {
        let payload = queue.de_q();

        //println!("Sending message to client: {}", payload);
        match stream.write_all(payload.as_bytes()) {
            Ok(()) => {
                let _ =stream.flush();
            },
            Err(err) => {
                eprintln!("Failed to send message to client: {}", err);
                break;
            }
        }
    }
}
