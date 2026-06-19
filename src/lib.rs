use clap::{Parser, Subcommand};
use std::net::SocketAddr;

pub const ALPN: &[u8] = b"acp-iroh-proxy/0";

#[derive(Parser)]
#[command(name = "acp-proxy", about = "P2P proxy for ACP (Agent Client Protocol) over Iroh")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Agent side: listens on Iroh P2P and proxies to a local ACP agent.
    ///
    /// By default, spawns the ACP agent as a subprocess (stdio transport).
    /// Use --local-acp to connect to an already-running TCP-based ACP server instead.
    Agent {
        /// Command to spawn the ACP agent subprocess (e.g., "acp-agent serve").
        /// For each incoming bi-directional stream, a new agent process is spawned and its
        /// stdin/stdout are proxied over the P2P stream.
        #[arg(long)]
        agent_cmd: Option<String>,

        /// TCP address of an already-running ACP server (e.g., 127.0.0.1:8765).
        /// Use this for HTTP/WebSocket-based ACP servers instead of spawning via --agent-cmd.
        #[arg(long)]
        local_acp: Option<SocketAddr>,

        /// Optional secret key (hex-encoded 32 bytes). Generates a random one if not provided.
        #[arg(long)]
        secret_key: Option<String>,
    },
    /// Client side: proxies a local ACP client over Iroh P2P to a remote agent-side proxy.
    ///
    /// By default, uses stdin/stdout so the ACP client can spawn this proxy as a subprocess.
    /// Use --local-bind to listen on a local TCP port instead (for clients that connect via TCP).
    Client {
        /// Endpoint ID (public key) of the remote agent-side proxy.
        #[arg(long)]
        remote_id: String,

        /// Relay URL used by the remote endpoint.
        #[arg(long)]
        relay_url: String,

        /// Direct UDP addresses of the remote endpoint (optional but recommended for hole-punching).
        #[arg(long, value_delimiter = ',')]
        addrs: Vec<SocketAddr>,

        /// If set, listen on this local TCP address for ACP clients to connect to
        /// (e.g., 127.0.0.1:9000). Otherwise, proxy via stdin/stdout (single connection).
        #[arg(long)]
        local_bind: Option<SocketAddr>,

        /// Optional secret key (hex-encoded 32 bytes). Generates a random one if not provided.
        #[arg(long)]
        secret_key: Option<String>,
    },
}
