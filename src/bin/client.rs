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
    loop {
        let n = match read_socket.read(&mut buffer).await {
            Err(_) => {
                println!("Server disconnected.");
                std::process::exit(1);
            }
            Ok(0) => {
                println!("Server disconnected.");
                std::process::exit(0);
            }
            Ok(n) => n,
        };

        let message = str::from_utf8(&buffer[..n]).unwrap();
        print!("{}", message);
        io::stdout().flush().unwrap();
    }
}
