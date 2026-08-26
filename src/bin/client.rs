use crossterm::{
    QueueableCommand,
    cursor::{MoveTo, MoveToNextLine, MoveToPreviousLine, MoveToRow},
    event::{Event, EventStream, KeyCode, KeyModifiers},
    execute,
    terminal::{self, Clear, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::StreamExt;
use std::io::{self, Write};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    select,
    sync::mpsc,
};

struct TermData {
    size: (u16, u16),
    prompt: String,
    content: Vec<String>,
}

enum EventAction {
    Continue,
    SubmitPrompt(String),
    Quit,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    terminal::enable_raw_mode()?;

    let socket = TcpStream::connect("127.0.0.1:1216").await?;
    let (read_socket, mut write_socket) = socket.into_split();
    let mut buf_reader = BufReader::new(read_socket);

    let (stdin_sender, mut stdin_receiver) = mpsc::unbounded_channel::<String>();

    tokio::spawn(async move {
        while let Some(message) = stdin_receiver.recv().await {
            write_socket.write_all(message.as_bytes()).await?;
        }

        Ok::<(), io::Error>(())
    });

    let mut term_data = TermData {
        size: terminal::size()?,
        prompt: String::new(),
        content: vec![],
    };

    let mut reader = EventStream::new();

    let mut quit = false;
    while !quit {
        let mut line = String::new();
        select! {
            maybe_event = reader.next() =>{
                match handle_event(maybe_event, &mut term_data) {
                    Ok(EventAction::SubmitPrompt(message)) => {
                        let _ = stdin_sender.send(message).map_err(|e| {
                            eprintln!("Error: {e}");
                        });
                    }
                    Ok(EventAction::Quit) => quit = true,
                    Ok(EventAction::Continue) => {},
                    Err(e) => eprintln!("Error: {e}"),
                }
            }

            read_result = buf_reader.read_line(&mut line) => {
                let bytes_read = read_result?;
                if bytes_read == 0 {
                    quit = true;
                }
                term_data.content.push(line.trim().to_string());
                eprintln!("{:?}", term_data.content);
            }
        };

        draw(&stdout, &term_data)?;
    }

    terminal::disable_raw_mode()?;
    execute!(stdout, LeaveAlternateScreen)?;
    Ok(())
}

fn handle_event(
    maybe_event: Option<io::Result<Event>>,
    term_data: &mut TermData,
) -> Result<EventAction, io::Error> {
    let event = match maybe_event {
        Some(Ok(event)) => event,
        Some(Err(e)) => {
            return Err(e);
        }
        None => return Ok(EventAction::Quit),
    };

    match event {
        Event::Key(event) => match event.code {
            KeyCode::Char(ch) => {
                if event.modifiers.contains(KeyModifiers::CONTROL) {
                    if ch == 'c' {
                        return Ok(EventAction::Quit);
                    }
                } else {
                    term_data.prompt.push(ch);
                }
            }
            KeyCode::Enter => {
                term_data.content.push(term_data.prompt.clone());
                term_data.prompt.push('\n');
                let ret = Ok(EventAction::SubmitPrompt(term_data.prompt.clone()));
                term_data.prompt.clear();
                return ret;
            }
            _ => (),
        },
        Event::Resize(new_cols, new_rows) => {
            term_data.size.0 = new_cols;
            term_data.size.1 = new_rows;
            return Ok(EventAction::Continue);
        }
        Event::Paste(str) => term_data.prompt.push_str(&str),
        _ => (),
    }

    Ok(EventAction::Continue)
}

fn draw<T: Write>(mut stream: T, term_data: &TermData) -> io::Result<()> {
    let (cols, rows) = term_data.size;

    stream.queue(Clear(terminal::ClearType::All))?;

    stream.queue(MoveTo(0, rows - 3))?;
    for message in term_data.content.iter().take((rows - 2) as usize).rev() {
        stream.write_all(message.as_bytes())?;
        stream.queue(MoveToPreviousLine(1))?;
    }

    stream.queue(MoveToRow(rows - 2))?;
    let bar_char = "═";
    stream.write_all(bar_char.repeat(cols as usize).as_bytes())?;
    stream.queue(MoveToNextLine(1))?;
    stream.write_all(term_data.prompt.as_bytes())?;

    stream.flush()?;
    Ok(())
}
