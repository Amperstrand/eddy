# Mutinynet custommsg transport issue (2026-08-24, UNRESOLVED)

## Symptom

custommsg messages are "sent to connectd for delivery" but NEVER
received by the peer's plugin on the mutinynet hub↔swap pair —
despite both nodes running the eddy-cln plugin, being connected
peers with 2 active channels, and the identical message type (59903)
working perfectly on the regtest ring.

## What was ruled out

- Plugin loading: both nodes show eddy-status working
- Peer connectivity: hub↔swap connected=True, 2 channels CHANNELD_NORMAL
- Message format: same 2-byte type prefix used on regtest
- Both directions tested (hub→swap AND swap→hub) — neither arrives

## Hypotheses (untested)

1. The hold plugin loaded via config on the hub interferes with the
   custommsg hook (hook priority/conflict)
2. The Docker network path between the mutinynet containers drops
   or reorders the peer messages in a way that affects custommsg
   (unlikely — payments work)
3. A CLN configuration option in the mutinynet lightning-config
   disables custommsg delivery (no obvious candidate in the config)
4. Version-specific: the mutinynet hub uses inr2-cln-eddy (built on
   inr2-cln-main which includes CLBOSS patches) — the CLBOSS patches
   may affect peer message handling

## Next steps

- Test custommsg between the mutinynet swap and a THIRD node without
  the hold plugin or CLBOSS patches
- Try `dev-sendcustommsg` or raw connectd debug
- Compare lightningd debug logs during message send on both regtest
  and mutinynet rings

## Impact

D1 (mutinynet CLN proof) is blocked by this issue. The CLN proof
remains valid from regtest + signet (both live). This is a deployment
debugging issue, not a protocol issue.
