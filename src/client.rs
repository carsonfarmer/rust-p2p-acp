use std::{net::SocketAddr, str::FromStr};

use acp_iroh_proxy::ALPN;
use anyhow::Context;
use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, RelayUrl, SecretKey,
    endpoint::presets,
};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

/// Run the client-side proxy.
///
/// If `local_bind` is provided, listens on a local TCP port for multiple ACP clients.
/// Otherwise, uses stdin/stdout (designed to be spawned by the ACP client as a subprocess).
pub async fn run(
    remote_id: String,
    relay_url: String,
    addrs: Vec<SocketAddr>,
    local_bind: Option<SocketAddr>,
    secret_key_hex: Option<String>,
) -> anyhow::Result<()> {
    let secret_key = parse_or_generate_secret_key(secret_key_hex)?;
    let endpoint_id = secret_key.public();
    info!("Client proxy starting with endpoint ID: {endpoint_id}");

    let remote_id = EndpointId::from_str(&remote_id).context("invalid remote endpoint ID")?;
    let relay_url = RelayUrl::from_str(&relay_url).context("invalid relay URL")?;

    let mut remote_addr = EndpointAddr::new(remote_id).with_relay_url(relay_url);
    for addr in &addrs {
        remote_addr = remote_addr.with_ip_addr(*addr);
    }

    info!("Remote agent endpoint ID: {remote_id}");
    if addrs.is_empty() {
        warn!("No direct addresses provided — will rely on relay (add --addrs for hole-punching)");
    }

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec()])
        .relay_mode(RelayMode::Default)
        .bind()
        .await
        .context("failed to bind iroh endpoint")?;

    endpoint.online().await;
    info!("Iroh endpoint online.");

    match local_bind {
        Some(bind_addr) => run_tcp_listener(endpoint, remote_addr, bind_addr).await,
        None => run_stdio(endpoint, remote_addr).await,
    }
}

/// Stdio mode: proxy a single stdin/stdout connection over Iroh.
///
/// Designed to be spawned by an ACP client as a subprocess transport.
/// Uses concurrent bidirectional copy — stdin stays open for the session lifetime.
async fn run_stdio(
    endpoint: Endpoint,
    remote_addr: EndpointAddr,
) -> anyhow::Result<()> {
    info!("Stdio mode: proxying stdin/stdout ↔ Iroh P2P. Press Ctrl+C to stop.");

    let conn = endpoint
        .connect(remote_addr, ALPN)
        .await
        .context("failed to connect to remote agent over Iroh")?;

    let (mut iroh_send, mut iroh_recv) = conn
        .open_bi()
        .await
        .context("failed to open bi-directional Iroh stream")?;

    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();

    // Concurrent bidirectional copy: stdin→iroh and iroh→stdout run in parallel.
    // When stdin closes (EOF), signal half-close on iroh_send and keep draining iroh→stdout.
    let stdin_to_iroh = tokio::spawn(async move {
        let n = tokio::io::copy(&mut stdin, &mut iroh_send).await;
        if let Err(e) = n { warn!("stdin→iroh: {e:#}"); }
        if let Err(e) = iroh_send.finish() { warn!("iroh finish: {e:#}"); }
    });

    let iroh_to_stdout = tokio::spawn(async move {
        let n = tokio::io::copy(&mut iroh_recv, &mut stdout).await;
        if let Err(e) = n { warn!("iroh→stdout: {e:#}"); }
    });

    // Wait for both to finish (stdin EOF + agent response complete).
    let _ = tokio::join!(stdin_to_iroh, iroh_to_stdout);

    conn.close(0u32.into(), b"done");
    endpoint.close().await;
    info!("Client proxy stopped.");
    Ok(())
}

/// TCP listener mode: listen on a local TCP port, proxy each connection over Iroh.
async fn run_tcp_listener(
    endpoint: Endpoint,
    remote_addr: EndpointAddr,
    bind_addr: SocketAddr,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .context("failed to bind local TCP listener")?;

    info!("TCP mode: listening on {bind_addr} — ACP clients connect here.");
    info!("Proxying TCP {bind_addr} → Iroh P2P → remote agent. Press Ctrl+C to stop.");

    let endpoint = &endpoint;
    let remote_addr = &remote_addr;

    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                info!("Shutting down...");
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((tcp_stream, peer_addr)) => {
                        info!("New TCP connection from {peer_addr}");
                        let ep = endpoint.clone();
                        let raddr = remote_addr.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_tcp_connection(ep, raddr, tcp_stream).await {
                                error!("Proxy error for {peer_addr}: {e:#}");
                            }
                            info!("TCP connection from {peer_addr} closed");
                        });
                    }
                    Err(e) => error!("Failed to accept TCP connection: {e}"),
                }
            }
        }
    }

    endpoint.close().await;
    info!("Client proxy stopped.");
    Ok(())
}

/// Handle one TCP connection: open a bi-directional stream on the existing Iroh connection.
/// The Iroh QUIC connection is reused across multiple TCP clients — each gets its own stream.
async fn handle_tcp_connection(
    endpoint: Endpoint,
    remote_addr: EndpointAddr,
    tcp_stream: tokio::net::TcpStream,
) -> anyhow::Result<()> {
    // ponytail: reuse the QUIC connection (iroh caches per-endpoint under the hood)
    let conn = endpoint
        .connect(remote_addr, ALPN)
        .await
        .context("failed to open Iroh connection")?;

    let (iroh_send, iroh_recv) = conn
        .open_bi()
        .await
        .context("failed to open bi-directional Iroh stream")?;

    let (tcp_read, tcp_write) = tokio::io::split(tcp_stream);
    copy_both(iroh_send, iroh_recv, tcp_read, tcp_write).await?;

    conn.close(0u32.into(), b"done");
    Ok(())
}

/// Pump bytes between an Iroh stream pair and a split TCP stream.
async fn copy_both(
    mut iroh_send: iroh::endpoint::SendStream,
    mut iroh_recv: iroh::endpoint::RecvStream,
    mut tcp_read: tokio::io::ReadHalf<tokio::net::TcpStream>,
    mut tcp_write: tokio::io::WriteHalf<tokio::net::TcpStream>,
) -> anyhow::Result<()> {
    let iroh_to_tcp = tokio::spawn(async move {
        let n = tokio::io::copy(&mut iroh_recv, &mut tcp_write).await;
        if let Err(e) = n { warn!("iroh→tcp: {e:#}"); }
        drop(tcp_write);
    });

    let tcp_to_iroh = tokio::spawn(async move {
        let n = tokio::io::copy(&mut tcp_read, &mut iroh_send).await;
        if let Err(e) = n { warn!("tcp→iroh: {e:#}"); }
        if let Err(e) = iroh_send.finish() { warn!("iroh finish: {e:#}"); }
    });

    tokio::select! {
        _ = iroh_to_tcp => {},
        _ = tcp_to_iroh => {},
    }

    Ok(())
}

fn parse_or_generate_secret_key(hex: Option<String>) -> anyhow::Result<SecretKey> {
    match hex {
        Some(s) => {
            let bytes = hex::decode(s).context("secret key must be valid hex")?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("secret key must be exactly 32 bytes"))?;
            Ok(SecretKey::from_bytes(&arr))
        }
        None => Ok(SecretKey::generate()),
    }
}
