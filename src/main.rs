mod agent;
mod client;

use acp_iroh_proxy::{Cli, Command};
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("acp_iroh_proxy=info".parse()?))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Agent {
            agent_cmd,
            local_acp,
            secret_key,
        } => {
            agent::run(agent_cmd, local_acp, secret_key).await?;
        }
        Command::Client {
            remote_id,
            relay_url,
            addrs,
            local_bind,
            secret_key,
        } => {
            client::run(remote_id, relay_url, addrs, local_bind, secret_key).await?;
        }
    }

    Ok(())
}
