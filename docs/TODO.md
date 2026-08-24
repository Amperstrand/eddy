# TODO — eddy / eddy-cln

## Done
- [x] LND: convergence proven (0.0002 residual, both topologies)
- [x] LND: arming race root-caused (#1) + fee scaling (#2)
- [x] LND: endurance + random-traffic cycle rate (#3, #4)
- [x] CLN: T1 zero-fee circulation PROVEN on regtest
  (`docs/reports/2026-08-23-eddy-cln-first-circulation.md`)

## Next
- [ ] Signet: deploy eddy-cln to inr2 ring (hub/swap/clboss), first
      live 20k-sat zero-fee circulation
- [ ] Mutinynet deploy (same plugin, env-separated)
- [ ] T1 trigger wired to broker drift monitors (auto-rebalance)
- [ ] T2 standing solver (eddy parity mode) on CLN
- [ ] Waiver amount-gating: only waive HTLCs within the circulation's
      range during the window (v2 safety)
- [ ] Upstream contribution: file the arming-race fix as a PR once our
      fork's pre-arm variant is validated (design in #5)

## 2026-08-24 session additions
- [x] B2: free-riding confirmed but self-limiting (see docs/reports/)
- [x] B4: non-participant = clean timeout, waivers self-heal
- [x] B5: exposure ∝ actual routed traffic, not window size
- [x] A1: htlc_accepted cannot modify forwarded HTLCs — no CLN
      RESUME_MODIFIED equivalent; setchannel remains best; reject-gating
      is the upgrade path
- [ ] Mutinynet custommsg transport issue (unresolved — see
      docs/reports/mutinynet-custommsg-issue.md)
- [ ] D2: LND race on signet (funding chain is slow; signet blocks
      needed for LND wallet confirms)
