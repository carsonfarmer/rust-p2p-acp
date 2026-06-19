# acp-iroh-proxy

P2P proxy for [ACP](https://agentclientprotocol.com) over [Iroh](https://iroh.computer). Lets a remote ACP client talk to a locally-running ACP agent through Iroh's relay + hole-punching network.

```
ACP Client ──spawns──▶ [client proxy stdin/stdout] ──Iroh P2P──▶ [agent proxy] ──spawns──▶ ACP Agent (pi-acp)
```

No ACP awareness needed — the proxy shuttles raw bytes. Multiple clients can share one agent process.

## Quick test with a friend

### Your side (agent — stays running)

```bash
git clone git@github.com:carsonfarmer/rust-p2p-acp.git
cd rust-p2p-acp
cargo build --release
./target/release/acp-proxy agent --agent-cmd "pi-acp"
```

It prints something like:

```
=== Give this to the remote client ===
acp-proxy client \
  --remote-id 68c68d8705b91... \
  --relay-url https://usw1-1.relay.n0.iroh.link./
  --addrs 162.156.55.169:36853,...
```

**Copy those three lines and send them to your friend.**

### Friend's side (Mexico, or anywhere)

**Step 1 — get the binary.** Either you build and send it, or they build from source:

```bash
git clone git@github.com:carsonfarmer/rust-p2p-acp.git
cd rust-p2p-acp
cargo build --release
```

**Step 2 — install acpx (the ACP client):**

```bash
npm install -g acpx
```

**Step 3 — configure acpx.** Paste the three lines you received into the config:

```bash
cat > ~/.acpx/config.json << 'JSON'
{
  "defaultAgent": "mica",
  "agents": {
    "mica": {
      "command": "/path/to/acp-proxy client --remote-id <THEIR_ID> --relay-url <THEIR_URL> --addrs <THEIR_ADDRS>"
    }
  }
}
JSON
```

**Step 4 — test:**

```bash
acpx mica exec "Hello from Mexico! What is 10+10?"
```

They should see pi respond with `20`.

### Quick byte-level test (no acpx needed)

On your side, temporarily switch to `cat`:

```bash
./target/release/acp-proxy agent --agent-cmd "cat"
```

Friend runs:

```bash
echo "hello from mexico" | ./acp-proxy client \
  --remote-id <ID> --relay-url <URL> --addrs <ADDRS>
# Should echo back: hello from mexico
```

## CLI reference

### `acp-proxy agent`

| Flag | Description |
|---|---|
| `--agent-cmd <CMD>` | Spawn ACP agent subprocess per stream. Stdio transport. |
| `--local-acp <ADDR>` | Connect to already-running TCP-based ACP server instead. |
| `--secret-key <HEX>` | 64-char hex secret key. Random if not provided. |

### `acp-proxy client`

| Flag | Description |
|---|---|
| `--remote-id <ID>` | Agent's endpoint ID (public key). **Required.** |
| `--relay-url <URL>` | Agent's relay URL. **Required.** |
| `--addrs <A,B,...>` | Direct addresses for hole-punching (recommended, not required). |
| `--local-bind <ADDR>` | Listen on TCP for multiple ACP clients. Default: stdin/stdout. |
| `--secret-key <HEX>` | 64-char hex secret key. Random if not provided. |

## Architecture

- **Agent side:** `Iroh Router` + `ProtocolHandler` → accepts QUIC connections. Each bi-directional stream attaches to a shared `SessionManager` that owns one subprocess. Fan-in merges all client input to subprocess stdin; fan-out broadcasts subprocess stdout to all connected clients via a `tokio::broadcast` channel.

- **Client side:** stdin/stdout mode (spawned by ACP client) or TCP listener mode. `tokio::join!` on concurrent bidirectional `copy` tasks with proper QUIC half-close (`finish()`) signalling.

- **Relay + hole-punching:** `RelayMode::Default` — uses iroh's public relays. Pass `--addrs` for direct hole-punching optimization.

## Requirements

- Rust 1.91+ (see `rust-toolchain.toml`)
- `pi-acp` or any ACP-compatible agent on the agent side
- `acpx` or any ACP-compatible client on the client side
