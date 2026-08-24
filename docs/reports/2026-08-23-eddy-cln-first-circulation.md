# eddy-cln: FIRST ZERO-FEE COOPERATIVE CIRCULATION ON CORE LIGHTNING

**Run `89ccf56d` — regtest 3-node ring — 2026-08-23 19:00 UTC**

50,000,000 msat (50k sats) circulated n1→n2→n3→n1 and back to the
initiator with **zero routing fees**:

- `amount_msat == amount_sent_msat == 50000000` (see
  `evidence/regtest-20260823/pay-proof-89ccf56d.json`)
- invoice status: `paid`
  (`evidence/regtest-20260823/invoice-proof-89ccf56d.json`)
- all three channels' fee policies restored to their pre-circulation
  baseline (1 msat / 10 ppm) by the watchdog —
  `evidence/regtest-20260823/channels-n*.json`

## Why this matters (vs upstream eddy on LND)

Upstream eddy's fee waiver rides LND's HTLC interceptor
(`RESUME_MODIFIED`). We measured (issues #1, #4) that the initiator
fires the payment **~1 ms** after the last hop accepts the proposal —
before any hop can arm its interceptor. Result: every hop charges its
normal fee, deterministically (E5: identical fee totals to the msat
across three cycles).

The CLN port replaces the interceptor with `setchannel` fee
transitions: each hop zeroes its channel's fees when it accepts the
proposal and restores them when the circulation settles (or a 5 s
watchdog expires the waiver). State transitions are serialized by the
node — there is no race to lose. Free means free.

## Trigger

T1 (on-demand RPC):

```
lightning-cli eddy-rebalance <scid> <amount_sat>
```

The operator picks the drifted channel; the plugin builds the
beneficial cycle, arms waivers hop-by-hop via custommsg (kind 59903,
odd experimental range), fires a self-payment around the ring, and
restores fees on settle/failure.

## Live-earned fixes (full list in the plugin's commit)

- custommsg wire types are 2 bytes: 88901 is unframeable (parsed as
  WIRE_WARNING type 1); malformed frames can FATAL connectd/channeld
- `sendcustommsg` payload must include the type prefix
- pyln `init()` signature must be exactly `(options, configuration, plugin)`
- `plugin-dir` loads every file — a shell script beside the plugin
  killed lightningd at getmanifest
- sendpay: msat strings + `style: tlv` + `payment_secret` as a
  top-level kwarg; delays strictly descending
- teardown needs a real watchdog thread — RPC-piggybacked restore
  never fires on idle nodes

## Next

Signet ring on existing inr2 nodes (hub/swap/clboss), 20k-sat
federation-minimum scale, T1 wired to the broker's drift monitors.

## Signet: FIRST LIVE ZERO-FEE CIRCULATION (2026-08-23 20:50 UTC)

Run `887dd5c3` on the inr2 production ring (cln-hub → cln-swap →
cln-clboss → cln-hub), 2,000,000 msat:

- `amount_msat == amount_sent_msat == 2000000`, status `complete`,
  invoice `paid` (`evidence/signet-20260823/proof-887dd5c3.json`)
- every sender-side channel balance dropped exactly 2M and every
  receiver side gained it — the cooperative rebalance moved real signet
  liquidity at zero routing cost
- waivers armed hop-by-hop and restored (swap's `319055x18x0` armed at
  propose, watchdog-restored cleanly)

Live-earned fix on the way: the ring-closing channel preference could
echo the channel a hop RECEIVED the proposal on (swap armed its
incoming hub channel instead of its outgoing clboss channel) — nodes
with multiple channels to the initiator need the incoming channel
explicitly excluded from selection.
