# eddy

Free circular rebalancing with no central coordinator.


## Overview

In René Pickhardt's magnum opus, ["A Mathematical Theory of Payment Channel Networks"](https://arxiv.org/pdf/2601.04835), he proved that, even in a perfectly circular economy, asymmetric channel fees will make the network more expensive and less reliable over time. One of the mitigation strategies he listed is coordinated replenishment. 

The optimal version of this requires a central coordinator with full knowledge of every node's balances. In practice, any circulation decomposes into simple cycles, and each cycle can be executed independently. That's what eddy does.

eddy is a peer-to-peer daemon that runs alongside LND. Nodes gossip their imbalances and execute one cycle at a time. Repeated cycles converge toward balanced channels. No central server, no trust, no fees.

## How It Works

eddy nodes form a cooperative protocol. Each node shares its channel imbalances (how far each channel deviates from 50/50) with peers via gossip. When a node detects a cycle where every participant would benefit from pushing liquidity in the same direction, it proposes a circulation:

```
     propose          propose          propose
 Alice ───────► Bob ───────► Carol ───────► Alice
   │                                          │
   │              execute HTLCs               │
   │◄─────────────────────────────────────────┘
   │
   └──► settle (Alice knows the preimage)
```

The protocol has 2 phases:

1. **Propose** — Initiator builds the route, creates an invoice, and sends a proposal (including the payment hash) hop-by-hop around the ring. Each node validates that the push would improve its balance and forwards the proposal to the next hop. If any node rejects, the proposal aborts.
2. **Execute** — When the proposal completes the ring back to the initiator, the initiator executes the payment along the pre-built route. Each intermediate node's HTLC interceptor modifies the forwarded HTLC to waive its routing fee, so the circulation is free. The initiator holds the preimage, so settlement propagates back from it.

## Architecture

| Package | Description |
|---|---|
| `cmd/eddyd` | Main daemon: config, event loops, solver |
| `internal/circulation` | Propose/execute protocol and HTLC interceptor |
| `internal/graph` | Local graph of channel imbalances, DFS cycle detection |
| `internal/gossip` | Balance-state dissemination with TTL flooding |
| `internal/p2p` | Custom message transport over LND's peer messaging |
| `internal/lnd` | gRPC client wrapper for LND |
| `internal/shortid` | Truncated pubkey display helper |

The daemon runs several concurrent loops: a **solver loop** that periodically scans for rebalancing cycles, a **gossip heartbeat** that shares local state, a **channel event loop** and **HTLC event loop** that react to LND events, and an **HTLC interceptor** that waives fees and resumes forwarded HTLCs for in-flight circulations.

## Getting Started

### Prerequisites

- Go 1.24+
- LND 0.19.3-beta (with gRPC enabled)

### Configuration

Create a `config.yaml` (all fields optional — defaults shown):

```yaml
log_level: info        # debug | info | warn | error
data_dir: ~/.eddy      # state and log files

solver:
  max_cycle_length: 5    # max nodes in a rebalancing cycle
  stale_threshold: 60    # seconds before gossip data is considered stale
  min_push_amount: 100000  # minimum rebalance amount (sats)

circulation:
  timeout: 30s           # protocol message timeout
  payment_timeout: 60s   # HTLC payment timeout

gossip:
  interval: 30s          # how often to broadcast local state
```

### Running

```bash
go build -o eddyd ./cmd/eddyd/

./eddyd \
  --lnd-host localhost:10001 \
  --lnd-cert /path/to/tls.cert \
  --lnd-macaroon /path/to/admin.macaroon
```

Optionally point to a config file with `--config path/to/config.yaml`.

## Demo

The `eddy-demo/` directory includes a self-contained demo using [Polar](https://lightningpolar.com/) (Docker-based regtest Lightning network) and a TUI that visualizes the rebalancing in real time.

```bash
cd eddy-demo
./start.sh
```

This will:
1. Extract and start a 4-node Polar network (Alice, Bob, Carol, Dave)
2. Build `eddyd` and launch an instance on each node
3. Simulate payments that imbalance channels. 
4. Open a TUI showing channel balances, topology, and circulations as they happen.

Requires Docker and Go.

## Key Features

1. Only the **initiator** creates the invoice and holds the preimage - no other node can trigger settlement.
2. Intermediate nodes use `RESUME_MODIFIED` to waive fees and forward immediately - no HODL invoices required.
3. If any node rejects a proposal (busy, insufficient delta, unknown channel), the proposal aborts and no payment is attempted.
4. Rebalances are free. If any node does not waive fees, the payment will fail with `FEE_INSUFFICIENT`.

## Roadmap

- A mechanism to indicate "desired" channel balance rather than defaulting to 50/50. 
- Increase channel balance privacy.  
- Metrics and notifications(?).
- An interceptor registry for which this project, and others like it, would be a plugin. 
- A human audit. This project was 99% vibecoded.

## Disclaimer

This software is provided as-is with no guarantees. Use at your own risk.
