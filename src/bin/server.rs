use rand::RngExt;
use std::{collections::HashMap, io, net::SocketAddr};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};

enum ChatEvent {
    ClientConnected(SocketAddr, UnboundedSender<String>),
    ClientDisconnected(SocketAddr),
    NewMessage(SocketAddr, String),
}

struct Server {
    clients: HashMap<SocketAddr, UnboundedSender<String>>,
}

impl Server {
    fn new() -> Self {
        let clients = HashMap::new();
        Server { clients }
    }
}

async fn server(mut receiver: UnboundedReceiver<ChatEvent>) {
    let mut server = Server::new();

    while let Some(chat_event) = receiver.recv().await {
        match chat_event {
            ChatEvent::ClientConnected(peer_addr, write_channel) => {
                println!("{peer_addr} connected");
                server.clients.insert(peer_addr, write_channel);
            }
            ChatEvent::ClientDisconnected(sender_address) => {
                println!("{sender_address} disconnected");
                for channel in server.clients.values_mut() {
                    channel
                        .send(format!("{sender_address} disconnected\n"))
                        .unwrap_or_else(|e| eprintln!("Error: {e}"));
                }
                server
                    .clients
                    .retain(|client_address, _| *client_address != sender_address);
            }
            ChatEvent::NewMessage(sender_address, message) => {
                print!("{sender_address} says {message}");
                server.clients.retain(|client_address, channel| {
                    if *client_address == sender_address {
                        return true;
                    }
                    channel.send(message.clone()).is_ok()
                });
            }
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let (sender, receiver) = mpsc::unbounded_channel::<ChatEvent>();
    tokio::spawn(server(receiver));

    let listener = TcpListener::bind("127.0.0.1:1216").await?;
    let token: u128 = rand::rng().random();
    println!("Server token: {:X}", token);

    loop {
        let (mut socket, addr) = listener.accept().await?;

        println!("New request from {addr}");

        let sender = sender.clone();
        tokio::spawn(async move {
            if authenticate_client(&mut socket, token).await.is_err() {
                if socket
                    .write_all("Incorrect token, goodbye!\n".as_bytes())
                    .await
                    .is_err()
                {
                    return;
                };

                return;
            };

            if socket
                .write_all("Token accepted, welcome to the server!\n".as_bytes())
                .await
                .is_err()
            {
                return;
            };

            client(socket, sender).await;
        });
    }
}

async fn authenticate_client(mut socket: &mut TcpStream, token: u128) -> io::Result<()> {
    const TOKEN_PROMPT: &[u8] = b"Enter server token: ";

    socket.write_all(TOKEN_PROMPT).await?;

    let mut buf_reader = BufReader::new(&mut socket);
    let mut line = String::new();
    buf_reader.read_line(&mut line).await?;

    let inputted_token = u128::from_str_radix(line.trim(), 16)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    if inputted_token != token {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Invalid authentication token",
        ));
    }

    Ok(())
}

async fn client(socket: TcpStream, sender: UnboundedSender<ChatEvent>) {
    let Ok(addr) = socket.peer_addr() else { return };
    let (read_socket, mut write_socket) = socket.into_split();

    let (write_return, mut read_return) = mpsc::unbounded_channel::<String>();

    if sender
        .send(ChatEvent::ClientConnected(addr, write_return))
        .is_err()
    {
        return;
    }

    tokio::spawn(async move {
        while let Some(message) = read_return.recv().await {
            if write_socket.write_all(message.as_bytes()).await.is_err() {
                return;
            }
        }
    });

    let mut buf_reader = BufReader::new(read_socket);

    loop {
        let mut line = String::new();
        match buf_reader.read_line(&mut line).await {
            Ok(0) | Err(_) => {
                if sender.send(ChatEvent::ClientDisconnected(addr)).is_err() {
                    return;
                };
                return;
            }
            _ => (),
        };

        if sender.send(ChatEvent::NewMessage(addr, line)).is_err() {
            return;
        }
    }
}
