# eddy-cln — Validation Report & Replication Guide

**Amperstrand's CLN port of eddy, with the complete record of what was
validated, how, and how to reproduce every result.**

- Status: **zero-fee cooperative circular rebalancing PROVEN** — first on
  regtest, then on **live signet** (production channels)
- Evidence: [`evidence/`](../evidence/) (raw JSON from both proofs)
- Reports: [`docs/reports/`](reports/) (per-milestone writeups)
- Open findings on the LND original: issues
  [#1](https://github.com/Amperstrand/eddy/issues/1)–[#5](https://github.com/Amperstrand/eddy/issues/5)

---

## 1. What this is

eddy ([upstream](https://github.com/wactario/eddy), LND) rebalances
Lightning channels through **cooperation instead of fees**: nodes gossip
channel imbalances, find cycles where every participant benefits from
liquidity moving the same direction, and execute the circulation with
every hop waiving its routing fee. The economic premise is René
Pickhardt's *A Mathematical Theory of Payment Channel Networks*:
per-hop fees systematically drain circular economies, and coordinated
replenishment is the mitigation.

`eddy-cln` is our port of the propose/execute protocol to **Core
Lightning as a native plugin** (`cln/eddy_cln.py`, Python + pyln). One
design substitution:

| | upstream eddy (LND) | eddy-cln |
|---|---|---|
| Fee waiver | HTLC interceptor `RESUME_MODIFIED` (per-HTLC, racy) | **`setchannel <scid> 0 0` during the circulation window, restore after** (serialized state transition) |
| Peer messages | custom P2P messages | `custommsg` (kind **59903** / `0xE9FF`, odd experimental range) |
| Trigger | standing solver | **T1: `eddy-rebalance <scid> <amount_sat>` on-demand RPC** (standing solver is the T2 roadmap item) |

### Why the substitution matters

We ran upstream's protocol through 20 instrumented LND runs (14 on a
4-node ring, 7 on a 5-node ring — corpus and analysis in
[lightning-playground](https://github.com/Amperstrand/lightning-playground/tree/main/test-artifacts/eddy)).
Findings:

- **Convergence is real**: a single clean circulation restored balances
  to within 0.02–0.03% of perfect (skew 0.450 → 0.0003), settling in
  ~680 ms.
- **But "free" is false as implemented — structurally** (issue #1):
  the initiator fires the payment **~1 ms** after the last hop accepts
  the proposal, while interceptor arming needs more than that. Result:
  every forwarding hop charges (~5,500 msat/hop; fee totals identical
  *to the millisecond* across repeated cycles — deterministic, not
  timing luck).
- Fees scale linearly with cycle length (issue #2); wallet safety and
  endurance are clean (issues #3, #4); the fix design is issue #5.

CLN has no per-HTLC interceptor mutation. `setchannel` fee transitions
are **serialized by the node** — there is no race to lose. Free means
free.

## 2. What we validated

| # | Claim | Where proven | Evidence |
|---|---|---|---|
| 1 | The propose→waive→ring-close→execute chain works on CLN | regtest 3-ring, run `89ccf56d` | `evidence/regtest-20260823/` |
| 2 | **Zero routing fees on a full circulation** | regtest: 50,000,000 msat, `amount_msat == amount_sent_msat`; signet: 2,000,000 msat, same | `pay-proof-89ccf56d.json`, `proof-887dd5c3.json` |
| 3 | **Works on live signet, production channels** | inr2 ring hub→swap→clboss→hub, run `887dd5c3`, invoice `paid`, preimage confirmed | `evidence/signet-20260823/` |
| 4 | Liquidity actually moves (it is a real rebalance) | signet: every sender-side channel −2M msat, every receiver-side +2M | `proof-887dd5c3.json` `channels.*` |
| 5 | Fee restoration / safety | waivers armed at propose, restored on settle or 5 s watchdog; all channels back to baseline after both runs | channel states in both evidence dirs |
| 6 | Benefit gating | a hop rejects when forwarding would not move its channel toward balance (`reject ... not beneficial` observed live) | run logs |
| 7 | Atomic failure | failed circulations (sendpay rejected) restore fees, no funds stuck | regtest attempt log |

## 3. How we validated it (methodology)

**Instrument first, then iterate.** The LND race was invisible to
headline metrics — "mostly works, small fees" — and only fell to:

1. **Millisecond event correlation** across all daemons' logs (each
   protocol event matched with node + timestamp) — this produced the
   1 ms accept→fire signature that root-caused the race.
2. **Per-hop fee attribution**: `listpays`/`fwdinghistory` events mapped
   to named cycle edges, so *which* hop charged is explicit
   (`fee_charging_hops` in every results.json).
3. **Settle-proof counting**: only `COMPLETE`/`paid` states count as
   circulations (proposal/debug log lines don't).
4. **Balance deltas as ground truth**: a circulation is real iff
   sender-side balances drop and receiver-side rise by exactly the
   amount — checked on every leg of both proofs.
5. **Endurance repetition** on the LND side (3 consecutive skew→fix
   cycles; identical fee totals to the msat is what proved the race
   deterministic).

Every run's raw data is committed alongside its interpretation — no
claim on this page lacks a JSON behind it.

## 4. How to replicate

### 4.1 Regtest (any machine with Docker)

The lab lives in
[lightning-playground](https://github.com/Amperstrand/lightning-playground):
`docker/docker-compose.regtest.eddy.yml` (bitcoind + 3×CLN v26.06 with
the plugin bind-mounted) and `docker/eddy-cln/` (plugin, lab image
Dockerfile, `setup-ring.sh`).

```bash
git clone https://github.com/Amperstrand/lightning-playground
cd lightning-playground/docker

# 1. lab image: stock CLN v26.06 + python3 + pyln-client
#    (the stock image ships NO python — plugins cannot load without it)
docker build -t eddy-cln:v26.06 -f eddy-cln/Dockerfile eddy-cln/

# 2. bring up bitcoind + the 3-node ring
docker compose -f docker-compose.regtest.eddy.yml up -d

# 3. fund wallets + open the ring channels (idempotent)
bash eddy-cln/setup-ring.sh
# → n1–n2, n2–n3, n3–n1, 1M sat each, all single-sided (= rebalance fodder)

# 4. verify the plugin loaded on all three nodes
for n in n1 n2 n3; do
  docker exec eddy-$n lightning-cli --regtest plugin list | grep eddy_cln
done

# 5. T1 trigger — rebalance n1's drifted channel
docker exec eddy-n1 lightning-cli --regtest eddy-rebalance 311x3x0 50000

# 6. verify: zero fees, invoice paid, balances moved, fees restored
docker exec eddy-n1 lightning-cli --regtest listpays   # amount == amount_sent
docker exec eddy-n1 lightning-cli --regtest listinvoices | grep paid
for n in n1 n2 n3; do
  docker exec eddy-$n lightning-cli --regtest listpeerchannels
done
```

Expected: `amount_msat == amount_sent_msat` (zero routing fees), the
invoice `paid`, each sender-side spendable reduced by exactly the
circulation amount, all channel fee policies back at baseline.

### 4.2 Signet (real channels)

Same plugin, three real CLN nodes that already peer with each other:

1. **Plugin runtime**: each node's image needs `python3` +
   `pyln-client`. We layered it onto the existing images
   (`inr2-cln-eddy`, `inr2-clboss-eddy`); the recipe is the same
   two-line Dockerfile as the lab image.
2. **Deploy**: copy `eddy_cln.py` to a host dir (e.g. `/opt/eddy-cln/`,
   `chmod +x`), bind-mount it read-only into each node, and add
   `--plugin=/opt/eddy-cln/eddy_cln.py` to each node's command. This is
   purely additive — existing plugins (our swap provider runs on one of
   these nodes) are untouched.
3. **Ring**: open channels so a directed cycle exists
   (A→B, B→C, C→A). Ensure each sender side holds spendable funds —
   `listpeerchannels` `spendable_msat` per leg is the gate the plugin
   uses (`beneficial()` = forwarding still leaves the channel above
   half its capacity).
4. **Trigger** from any ring member:
   `lightning-cli --signet eddy-rebalance <outgoing-scid> <amount_sat>`
5. **Verify** exactly as in regtest (`listpays` amount==sent,
   `listinvoices` paid, per-leg balance deltas).

Our signet ring for reference: `cln-hub → cln-swap → cln-clboss →
cln-hub`, one reused 80k channel + two fresh 20k channels, circulation
2,000 sats.

### 4.3 Safety model (what to expect when things go wrong)

- Waivers carry a **30 s expiry** checked by a 5 s watchdog thread; a
  crashed circulation self-heals its fee policy.
- `eddy-restore-all` is the manual valve; restore failures log the
  exact `setchannel` command to run.
- A proposal rejected anywhere (not beneficial / channel gone) restores
  the initiator's waiver immediately.
- Malformed custommsg payloads are dropped without touching peer
  traffic (`result: continue`).

### 4.4 Known sharp edges (live-earned, all fixed in this code)

- custommsg wire types are **2 bytes** — a 4-hex-digit "type prefix"
  for values > 0xFFFF gets parsed as a different message type (we hit
  `WIRE_WARNING`) and malformed frames can crash `connectd`/`channeld`.
- `sendcustommsg` payload must include the type prefix; hex(json)
  alone is an invalid even message.
- pyln's `@plugin.init()` handler signature must be exactly
  `(options, configuration, plugin)` — any other third-arg name breaks
  kwargs binding.
- `--plugin-dir` loads *every* file in the dir — a shell script beside
  the plugin killed lightningd at `getmanifest`. Keep the plugin in its
  own directory.
- `sendpay` routes need msat **strings** (`"50000000msat"`),
  `style: "tlv"`, `payment_secret` as a **top-level kwarg**, and
  strictly descending delays.
- Hop selection must exclude the channel the proposal *arrived on*
  (nodes with several channels to the initiator otherwise echo it).

## 5. Where everything lives

| Artifact | Location |
|---|---|
| Plugin + lab image + ring setup | this branch, [`cln/`](../cln/) |
| Regtest proof (run `89ccf56d`) | [`evidence/regtest-20260823/`](../evidence/regtest-20260823/) |
| Signet proof (run `887dd5c3`) | [`evidence/signet-20260823/`](../evidence/signet-20260823/) |
| Per-milestone reports | [`docs/reports/`](reports/) |
| Backlog | [`docs/TODO.md`](TODO.md) |
| LND campaign corpus + analysis (20 runs) | lightning-playground `test-artifacts/eddy/` and `docs/experiments/eddy-notes.md` |
| Design doc (protocol mapping, trigger model) | lightning-playground `docs/experiments/eddy-cln-design.md` |

## 6. What's next

See [`docs/TODO.md`](TODO.md): mutinynet deploy, T1 wired to the
broker's drift monitors (auto-rebalance), the T2 standing solver for
eddy parity, amount-gated waivers, and — after our fork validates the
pre-arm variant — the upstream contribution fixing the LND race.
