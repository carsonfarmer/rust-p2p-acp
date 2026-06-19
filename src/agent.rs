use std::{net::SocketAddr, sync::Arc};

use acp_iroh_proxy::ALPN;
use anyhow::{Context, bail};
use iroh::{
    Endpoint, RelayMode, SecretKey,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use tokio::{
    net::TcpStream,
    sync::{Mutex, broadcast, mpsc},
};
use tracing::{error, info, warn};

pub async fn run(
    agent_cmd: Option<String>,
    local_acp: Option<SocketAddr>,
    secret_key_hex: Option<String>,
) -> anyhow::Result<()> {
    let mode = match (&agent_cmd, local_acp) {
        (Some(cmd), None) => AgentMode::Stdio { cmd: cmd.clone() },
        (None, Some(addr)) => AgentMode::Tcp { addr },
        _ => bail!("exactly one of --agent-cmd or --local-acp is required"),
    };

    let secret_key = parse_or_generate_secret_key(secret_key_hex)?;
    let endpoint_id = secret_key.public();
    let mode_desc = mode.description();
    info!("Agent proxy starting with endpoint ID: {endpoint_id}");
    info!("Mode: {}", mode_desc);

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec()])
        .relay_mode(RelayMode::Default)
        .bind()
        .await
        .context("failed to bind iroh endpoint")?;

    endpoint.online().await;
    let addr = endpoint.addr();

    let relay_url = addr.relay_urls().next().map(|u| u.to_string()).unwrap_or_default();
    let endpoint_id = endpoint.id().to_string();

    // Write info file to cwd for easy discovery.
    let info = serde_json::json!({
        "endpoint_id": endpoint_id,
        "relay_url": relay_url,
        "direct_addrs": addr.ip_addrs().map(|a| a.to_string()).collect::<Vec<_>>(),
        "alpn": "acp-iroh-proxy/0"
    });
    std::fs::write("acp-proxy-agent-info.json", serde_json::to_string_pretty(&info)?)?;

    // Print copy-paste client command to stderr (line-buffered on TTY).
    eprintln!("\n=== Give this to the remote client ===");
    eprintln!("acp-proxy client \\");
    eprintln!("  --remote-id {} \\", endpoint_id);
    eprintln!("  --relay-url {}", relay_url);
    if !addr.ip_addrs().next().is_none() {
        eprint!("  --addrs ");
        for a in addr.ip_addrs() { eprint!("{a},"); }
        eprintln!();
    }
    eprintln!("Info also written to ./acp-proxy-agent-info.json");
    eprintln!("==========================================\n");

    let session = Arc::new(SessionManager::new(mode));
    let handler = ProxyHandler { session };

    let router = Router::builder(endpoint.clone())
        .accept(ALPN, Arc::new(handler))
        .spawn();

    info!("Proxying Iroh P2P → {}. Ctrl+C to stop.", mode_desc);
    tokio::signal::ctrl_c().await.context("failed to listen for Ctrl+C")?;
    info!("Shutting down...");
    router.shutdown().await?;
    info!("Agent proxy stopped.");
    Ok(())
}

#[derive(Debug, Clone)]
enum AgentMode {
    Stdio { cmd: String },
    Tcp { addr: SocketAddr },
}

impl AgentMode {
    fn description(&self) -> String {
        match self {
            AgentMode::Stdio { cmd } => format!("subprocess `{cmd}`"),
            AgentMode::Tcp { addr } => format!("TCP {addr}"),
        }
    }
}

/// Shared session: one subprocess/TCP connection, fanned out to many Iroh clients.
/// All clients see the same output. All client input merges into the single session stdin.
#[derive(Debug)]
struct SessionManager {
    mode: AgentMode,
    stdout_tx: broadcast::Sender<Vec<u8>>,
    stdin_tx: mpsc::UnboundedSender<Vec<u8>>,
    stdin_rx: Mutex<Option<mpsc::UnboundedReceiver<Vec<u8>>>>,
    ready: Mutex<bool>,
}

impl SessionManager {
    fn new(mode: AgentMode) -> Self {
        let (stdout_tx, _) = broadcast::channel(256);
        let (stdin_tx, stdin_rx) = mpsc::unbounded_channel();
        Self { mode, stdout_tx, stdin_tx, stdin_rx: Mutex::new(Some(stdin_rx)), ready: Mutex::new(false) }
    }

    async fn ensure_started(self: &Arc<Self>) -> anyhow::Result<broadcast::Receiver<Vec<u8>>> {
        let mut ready = self.ready.lock().await;
        let rx = self.stdout_tx.subscribe();
        if *ready { return Ok(rx); }
        *ready = true;
        drop(ready);

        let stdin_rx = self.stdin_rx.lock().await.take()
            .context("stdin receiver already taken")?;

        match &self.mode {
            AgentMode::Stdio { cmd } => self.start_subprocess(cmd, stdin_rx).await?,
            AgentMode::Tcp { addr } => self.start_tcp(*addr, stdin_rx).await?,
        }
        Ok(rx)
    }

    async fn start_subprocess(
        self: &Arc<Self>, cmd: &str, mut stdin_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let mut child = tokio::process::Command::new("sh")
            .arg("-c").arg(cmd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn().context("spawn subprocess")?;

        let mut child_stdin = child.stdin.take().context("child stdin")?;
        let child_stdout = child.stdout.take().context("child stdout")?;

        // Pump: child stdout → broadcast to all clients.
        let stdout_tx = self.stdout_tx.clone();
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut r = child_stdout; let mut buf = [0u8; 4096];
            loop {
                match r.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => { if stdout_tx.send(buf[..n].to_vec()).is_err() { break; } }
                    Err(e) => { warn!("child stdout: {e:#}"); break; }
                }
            }
            info!("Child stdout closed — session ended.");
            drop(stdout_tx);
        });

        // Pump: stdin channel → child stdin. When all senders drop, close child stdin.
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            while let Some(data) = stdin_rx.recv().await {
                if let Err(e) = child_stdin.write_all(&data).await {
                    warn!("child stdin: {e:#}"); break;
                }
            }
            drop(child_stdin); // close stdin so child can exit
        });

        // Reap child, signal session end by dropping stdout_tx.
        let stdout_tx = self.stdout_tx.clone();
        tokio::spawn(async move {
            let s = child.wait().await;
            info!("Subprocess exited: {s:?}");
            drop(stdout_tx);
        });

        Ok(())
    }

    async fn start_tcp(
        self: &Arc<Self>, addr: SocketAddr, mut stdin_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let tcp = TcpStream::connect(addr).await.context("TCP connect")?;
        let (tcp_read, tcp_write) = tokio::io::split(tcp);

        let stdout_tx = self.stdout_tx.clone();
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut r = tcp_read; let mut buf = [0u8; 4096];
            loop {
                match r.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => { if stdout_tx.send(buf[..n].to_vec()).is_err() { break; } }
                    Err(e) => { warn!("TCP read: {e:#}"); break; }
                }
            }
            drop(stdout_tx);
        });

        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let mut w = tcp_write;
            while let Some(data) = stdin_rx.recv().await {
                if let Err(e) = w.write_all(&data).await { warn!("TCP write: {e:#}"); break; }
            }
        });

        Ok(())
    }

    fn stdin_sender(&self) -> mpsc::UnboundedSender<Vec<u8>> { self.stdin_tx.clone() }
}

#[derive(Debug, Clone)]
struct ProxyHandler { session: Arc<SessionManager> }

impl ProtocolHandler for ProxyHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_id = connection.remote_id();
        info!("New Iroh connection from {remote_id}");

        loop {
            let (iroh_send, iroh_recv) = match connection.accept_bi().await {
                Ok(s) => s,
                Err(e) => { info!("Connection from {remote_id} closed: {e:#}"); break; }
            };
            let session = self.session.clone();
            tokio::spawn(async move {
                if let Err(e) = attach(session, iroh_send, iroh_recv).await {
                    error!("Attach error: {e:#}");
                }
            });
        }
        connection.closed().await;
        Ok(())
    }
}

/// Attach one Iroh stream to the shared session.
/// Fan-in: Iroh recv → stdin channel. Fan-out: broadcast → Iroh send.
async fn attach(
    session: Arc<SessionManager>,
    mut iroh_send: iroh::endpoint::SendStream,
    mut iroh_recv: iroh::endpoint::RecvStream,
) -> anyhow::Result<()> {
    let mut stdout_rx = session.ensure_started().await?;
    let stdin_tx = session.stdin_sender();

    // Fan-in: Iroh recv → stdin channel.
    let stdin_tx2 = stdin_tx.clone();
    let fan_in = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match iroh_recv.read(&mut buf).await {
                Ok(Some(0)) => break,
                Ok(Some(n)) => { if stdin_tx2.send(buf[..n].to_vec()).is_err() { break; } }
                Ok(None) => continue,
                Err(e) => { warn!("iroh recv: {e:#}"); break; }
            }
        }
    });

    // Fan-out: broadcast → Iroh send. Runs until broadcast closes (session ends).
    let _fan_out = tokio::spawn(async move {
        loop {
            match stdout_rx.recv().await {
                Ok(data) => {
                    if let Err(e) = iroh_send.write_all(&data).await {
                        warn!("iroh send: {e:#}"); break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("client lagged {n} messages, skipping");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
        let _ = iroh_send.finish();
    });

    // Wait for client to finish sending (fan_in).
    // fan_out continues in background — it was moved into a spawned task
    // that will run until session ends (broadcast closes), then call
    // iroh_send.finish() to signal the client.
    let _ = fan_in.await;
    // fan_out is dropped here but the spawned task keeps running.
    Ok(())
}

fn parse_or_generate_secret_key(hex: Option<String>) -> anyhow::Result<SecretKey> {
    match hex {
        Some(s) => {
            let bytes = hex::decode(s).context("secret key must be valid hex")?;
            let arr: [u8; 32] = bytes.try_into()
                .map_err(|_| anyhow::anyhow!("secret key must be exactly 32 bytes"))?;
            Ok(SecretKey::from_bytes(&arr))
        }
        None => Ok(SecretKey::generate()),
    }
}
