# Regtest abuse experiments — B2/B4/B5 (2026-08-24)

## B2: Free-riding during waiver window — CONFIRMED

A background payment fired during an armed waiver window forwarded
through the waived channel at **fee=0 msat**. The 30s `setchannel 0 0`
window makes ALL traffic on that channel free, not just the
circulation's.

**Mitigation observed naturally**: the router often picks direct paths
that avoid the waived channel entirely (in our 3-ring, n1→n3 payments
go direct via 311x1x0 rather than through n2). Real-network exposure
≈ traffic that happens to route through that one channel in 30s.

## B4: Non-participant (defector) — SAFE

Stopping the plugin on n3 (becomes a silent non-participant):
- The propose message is dropped (no custommsg hook to receive it)
- The circulation stays in proposed/armed state on the earlier hops
- **Waivers restore via watchdog** (n1: 0 active, n2: 0 active after
  35s)
- No funds moved, no stuck state — clean timeout

## B5: Window-duration measurement — limited exposure

Six n1→n3 payments at 5s intervals during a 30s waiver window: only
1/6 actually routed through n2 (the others used the direct n1-n3
channel). The one that forwarded: fee=0 (free-ride confirmed).
**Practical exposure is proportional to actual routed traffic, not
the theoretical window.**

## Implications for the design

1. The free-riding is real but self-limiting in practice (routers
   avoid single-hop detours through the waived node).
2. The safety model (watchdog + atomic failure) holds under
   non-participant conditions.
3. Amount-gated waivers (only waive HTLCs near the circulation
   amount) remain the proper fix if exposure matters at scale.
