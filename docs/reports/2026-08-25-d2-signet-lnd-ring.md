# D2: UPSTREAM EDDY (LND) CIRCULATION PROVEN ON LIVE SIGNET

**Run `f0fdfb431dc00bd6` — signet 3-node LND ring (inr2, docker project
`lnd-signet-ring`) — 2026-08-25 21:23:19 UTC**

1,530 sats circulated l3→l1→l2→l3 and back to the initiator with **zero
real routing fees**, end-to-end in **382 ms** (cycle found 21:23:19.937 →
COMPLETE 21:23:20.319):

- payment `total_fees=0`, both route hops `fee_msat=0`
  (`evidence/signet-20260825-lndring/d2-l3-payments.json`)
- invoice SETTLED, `amt_paid == value == 1530 sat`, preimage verified
  (`d2-l3-invoices.json`)
- both intermediate hops' HTLC interceptors fired
  `RESUME_MODIFIED (waiving fees) … in=1530000 out=1530000
  faked_in=1531002` (`eddy-l1.log`, `eddy-l2.log`)

Stack: LND 0.20.0-beta, bitcoind /Satoshi:31.1.0/ (signet), upstream
eddyd with config `min_push_amount=500, gossip 10s, log_level=debug`.

## The phantom-fee bookkeeping artifact

`fwdinghistory` on l1 and l2 each book a **fee_msat=1002** entry for the
circulation. This is not a real fee: the interceptor satisfies LND's
required-fee validation by reporting a faked incoming amount
(1531002 = 1530000 + 1002), so LND's ledger *records* a fee while the
wire HTLCs carried 1530000 msat in and out at every hop.

Proof it was never retained — on-channel balance arithmetic, whole-sat
exact: every receiving edge went 5000 → 6530 (= 5000 + 1530 exactly); a
retained 1002 msat fee would show 6531. Anyone auditing forwarding
revenue during eddy circulations must subtract these phantom entries;
`total_fees`/`amt_paid` from the payment and invoice are the real
surface.

## Convergence

Rotational skew (l2→l1, l3→l2, l1→l3, 5000 sat each, direct channels)
left every push-direction edge local-heavy at 11530/5000 (delta +1530
over the 10000 capacity midpoint). The solver pushed exactly
`min(delta)=1530` around the cycle: push edges 11530 → 10000 (delta +0),
receiving edges 5000 → 6530. No positive deltas remain — the solver
correctly idles at "no cycles found". Zero wallet net change for every
node (l3 paid exactly 1530 to its own invoice around the ring).

## Race observation (D2's original question)

The upstream arming race (issue #1, E4's ~1 ms window) did **not**
reproduce in this run (n=1): the propose ring armed both interceptors at
19.950/19.952, the initiator fired at 19.954, and the HTLCs were
intercepted at 20.069/20.184 — hop arming happens at propose-accept,
which precedes payment dispatch by construction when the propose ring
completes before execution. The E4 races were observed on 4/5-node
regtest rings under different propose timing; more samples on this
topology would be needed to claim the window can't open.

## Ops notes (what it took to get here)

- **min_push_amount default is 100000 sat** — larger than the entire 20k
  channel capacity; the solver can never fire without lowering it.
- **Skew must be rotational**: `FindCycles` only traverses
  positive-delta (local-heavy) edges; two opposing payments self-cancel
  through direct channels (the phase-2 failure — both 5000-sat payments
  took the direct l1↔l3 edge and the solver correctly found nothing).
- A 15k payment over 10k-per-side channels fails NO_ROUTE (phase-1
  failure).
- eddyd logs to `data_dir/eddy.log` inside the container, not stdout —
  the empty `/tmp/eddyd.log` redirect was a red herring.
- Signet funding latency is real: the three channel-opens sat in mempool
  for ~70 blocks before activating (the original D2 blocker).
- eddyd delivery: `docker cp` into each container works; the compose
  override path does not (host-path exec fails).

## Reproduction

Ring: `docker compose -p lnd-signet-ring` on inr2 (lsr-l1/l2/l3, 20k
channels, 50/50 push). Scripts preserved verbatim:
`d2-resume.sh`, `d2-phase2.sh` (config + restart), `d2-phase3.sh`
(rotational skew + evidence capture). Raw evidence:
`evidence/signet-20260825-lndring/` (proof: `proof-f0fdfb43.json`).
