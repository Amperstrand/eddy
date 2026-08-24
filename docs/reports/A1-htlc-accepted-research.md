# A1: htlc_accepted hook research — per-HTLC waiver feasibility

## What the hook CAN do (per CLN PLUGINS.md docs)

The `htlc_accepted` hook fires when an HTLC arrives, BEFORE commitment.
Return values:
- `{"result": "continue"}` — forward/settle normally
- `{"result": "fail", "failure_message": "..."}` — reject the HTLC
- `{"result": "resolve", "payment_key": "<hex>"}` — settle immediately

## What it CANNOT do

**Modify the outgoing HTLC's amount or onion** — CLN builds the
outgoing HTLC from the incoming onion's payload; the hook has no
mechanism to change the forwarded amount. This is the fundamental
difference from LND's `RESUME_MODIFIED` which re-writes the outgoing
HTLC before forwarding.

## Per-HTLC waiver approaches (ranked)

### Option 1: REJECT-gating (viable, safe)
During the waiver window, the hook **rejects** any HTLC that is NOT
part of the active circulation (identified by payment_hash). The
circulation's HTLC forwards normally (it pays the standard fee);
non-circulation traffic gets a `temporary_channel_failure` and retries
after the window (fees restored).

Effect: no free-riding (other traffic pays normal fees or waits), but
the circulation itself pays fees. **This is compensated-rebalancing,
not free.**

### Option 2: resolve-and-accredit (complex)
Accept all HTLCs; the hook settles incoming ones immediately
(`resolve`). For the circulation's HTLCs, the node operator accredits
the fee difference out-of-band (manual accounting). Not practical for
automated rings.

### Option 3: paired with setchannel (current design)
The `setchannel 0 0` approach is actually the closest to what we want:
it waives fees for the circulation AND incidentally for anything else
on that channel in the window. The B2 experiment showed the
free-riding is real but self-limiting (routers avoid 1-hop detours).

## Conclusion

There is NO direct CLN equivalent to LND's RESUME_MODIFIED. The
`setchannel` waiver remains the best available mechanism. The
free-riding cost (B2: bounded by 30s window × actual routed traffic)
is the accepted trade-off. If it becomes a problem at scale,
reject-gating (Option 1) is the upgrade path — it trades the
free-riding for the cost of the circulation's own fees.
