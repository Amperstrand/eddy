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
