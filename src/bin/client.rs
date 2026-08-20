use std::io::{self, Write};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, tcp::OwnedReadHalf},
    sync::mpsc,
};

#[tokio::main]
async fn main() -> io::Result<()> {
    let socket = TcpStream::connect("127.0.0.1:1216").await?;
    let (read_socket, mut write_socket) = socket.into_split();

    tokio::spawn(receiver(read_socket));

    let (stdin_sender, mut stdin_receiver) = mpsc::unbounded_channel();

    std::thread::spawn(move || {
        let mut line = String::new();

        while io::stdin().read_line(&mut line).is_ok() {
            if stdin_sender.send(line.clone()).is_err() {
                break;
            }
            line.clear();
        }
    });

    while let Some(input) = stdin_receiver.recv().await {
        write_socket.write_all(input.as_bytes()).await?;
    }

    Ok(())
}

async fn receiver(mut read_socket: OwnedReadHalf) {
    let mut buffer = [0u8; 1024];
    while let Ok(n) = read_socket.read(&mut buffer).await {
        if n == 0 {
            break;
        }
        
        let message = String::from_utf8_lossy(&buffer[..n]);
        print!("{}", message);
        io::stdout().flush().unwrap();
    };

    println!("Server disconnected.");
    std::process::exit(0);
}
