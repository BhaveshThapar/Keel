use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "keel-admin", about = "Operate a running Keel cluster")]
struct Cli {
    /// Admin address of the current leader.
    #[arg(long)]
    admin: SocketAddr,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    TransferLeader { to: u64 },
    AddLearner { node: u64 },
    Promote { node: u64 },
    Remove { node: u64 },
    Snapshot,
}

impl Command {
    fn path(&self) -> String {
        match self {
            Self::TransferLeader { to } => format!("/transfer-leader?to={to}"),
            Self::AddLearner { node } => format!("/add-learner?node={node}"),
            Self::Promote { node } => format!("/promote?node={node}"),
            Self::Remove { node } => format!("/remove?node={node}"),
            Self::Snapshot => "/snapshot".into(),
        }
    }
}

fn run(cli: Cli) -> Result<String, String> {
    let mut stream = TcpStream::connect_timeout(&cli.admin, Duration::from_secs(2))
        .map_err(|error| format!("connect to {}: {error}", cli.admin))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| error.to_string())?;
    let path = cli.command.path();
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: keel\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    )
    .map_err(|error| error.to_string())?;
    stream.flush().map_err(|error| error.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| error.to_string())?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| "the node returned a malformed HTTP response".to_string())?;
    let status = head
        .lines()
        .next()
        .ok_or_else(|| "the node returned no HTTP status".to_string())?;
    if !status.contains(" 202 ") {
        return Err(format!("{status}: {}", body.trim()));
    }
    Ok(body.trim().to_string())
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(message) => {
            println!("{message}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
