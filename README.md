# eddy

Free circular rebalancing with no central coordinator.

## Research results (this fork)

This is a fork of [wactario/eddy](https://github.com/wactario/eddy) carrying the research branch `eddy-cln`, which asked whether eddy's fee-free circular rebalancing survives contact with real CLN interceptor semantics — specifically, whether each hop can arm its HTLC interceptor in time to waive its fee. Across an instrumented 20-run corpus, the central finding is that the arming race is structural, not timing luck ([#1](https://github.com/Amperstrand/eddy/issues/1)): the initiator fires the payment ~1ms after the last hop accepts, leaving each hop only ~1–5ms to arm, and at these latencies the charged-hop outcome is deterministic. Because every fee-charging hop is one lost race, total fees scale linearly with cycle length ([#2](https://github.com/Amperstrand/eddy/issues/2)) — ≈15,000 msat on K4 (3 hops) vs 21,988 msat on ring5, a constant ≈5.5k msat (≈0.00014%) per hop on ~4M-sat circulations. The protocol itself holds up: convergence was proven at both sizes (skew 0.450 → 0.0003 on ring5, 0.267 → 0.0002 on K4) with zero on-chain deltas in every run ([#4](https://github.com/Amperstrand/eddy/issues/4)), and three consecutive skew→converge cycles on one ring showed zero degradation, an identical 21,988 msat fee total every cycle, and no state drift ([#3](https://github.com/Amperstrand/eddy/issues/3)). A ~20-line fix is designed but not implemented ([#5](https://github.com/Amperstrand/eddy/issues/5)): arm the interceptor before forwarding proposal-accept, or add an armed-ack traversal costing one extra ~21ms ring pass vs the 680ms settle — either makes the waiver deterministic. Still open: implementing that fix, and the observation that fixable cycle existence depends on the drain pattern (2 of 3 random-drain seeds produced one; ~50/50-balanced starting states leave fewer).

| Finding | Evidence |
|---|---|
| The interceptor-arming race is structural: initiator fires ~1ms after last-hop accept, hops get ~1–5ms to arm, outcome deterministic at these latencies | [#1](https://github.com/Amperstrand/eddy/issues/1) |
| Total fees scale linearly with cycle length; per-hop rate constant (≈5.5k msat ≈ 0.00014% per hop) | [#2](https://github.com/Amperstrand/eddy/issues/2) |
| Endurance: 3 consecutive skew→converge cycles, zero degradation, fee total 21,988 msat identical every cycle | [#3](https://github.com/Amperstrand/eddy/issues/3) |
| Convergence + wallet safety PROVEN (skew → ~0, zero on-chain deltas); random drain creates fixable cycles in 2/3 seeds | [#4](https://github.com/Amperstrand/eddy/issues/4) |
| Fix design: ~20-line pre-arm on proposal-accept (or armed-ack traversal, +~21ms) — makes the fee waiver deterministic | [#5](https://github.com/Amperstrand/eddy/issues/5) |

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

## Donations

If you believe this project has value, please consider donating to this address:

bc1q45nt34zrkkw8fve0rdkk9h80e7kw6gjt50paer