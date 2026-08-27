# TODO — eddy / eddy-cln

## Done
- [x] LND: convergence proven (0.0002 residual, both topologies)
- [x] LND: arming race root-caused (#1) + fee scaling (#2)
- [x] LND: endurance + random-traffic cycle rate (#3, #4)
- [x] CLN: T1 zero-fee circulation PROVEN on regtest
  (`docs/reports/2026-08-23-eddy-cln-first-circulation.md`)
- [x] D2: upstream LND ring on live signet — zero-fee circulation
      PROVEN (run f0fdfb43); phantom-fee fwdinghistory artifact
      captured; arming race NOT reproduced (n=1) —
      `evidence/signet-20260825-lndring/`,
      `docs/reports/2026-08-25-d2-signet-lnd-ring.md`
- [x] Signet: eddy-cln live circulation PROVEN (run 887dd5c3, 2k sat,
      amount == sent, invoice paid) —
      `evidence/signet-20260823/proof-887dd5c3.json`. Standing deploy
      deliberately not kept: riders stripped from the prod fleet per
      the compose contract (2026-08-25); reload on demand.
- [x] Mutinynet: eddy-cln live circulation PROVEN (run d3b9a352, 5M
      msat via hub-swap ring) — `evidence/mutinynet-20260824/`
      (+ commit c70b80e resolves the custommsg red herring)

## Next
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
