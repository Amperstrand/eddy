#!/usr/bin/env python3
"""eddy-cln — cooperative circular rebalancing as a native CLN plugin.

Port of wactario/eddy (LND) to Core Lightning. Two departures:

1. Fee waiver via `setchannel` — a SERIALIZED state transition (zero
   feebase/feeppm for the circulation window, restore after), not a
   per-HTLC interceptor race. The LND implementation deterministically
   loses a ~1ms arming race (see Amperstrand/eddy#1); this design
   eliminates the race class rather than fixing its timing.

2. Trigger model T1 (primary): on-demand `eddy-rebalance` RPC for
   operator stacks; T2 (standing solver) available for parity.

Protocol messages ride custommsg (odd kind 88901 = eddy-cln range,
payload = JSON): PROPOSE (hop-by-hop ring build), ACCEPT (carries the
waiver commitment), EXEC (initiator fires sendpay).
"""
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from pyln.client import Plugin, RpcException, Millisatoshi

plugin = Plugin()

EDDY_MSG_KIND = 59903  # 2-byte odd type, BOLT#1 experimental range (0xE9FF)
WAIVER_WINDOW_S = 30   # fee-restore watchdog bound


@dataclass
class Waiver:
    scid: str
    feebase: int
    feeppm: int
    expires: float


@dataclass
class Circulation:
    cycle_id: str
    hops: list[dict]        # [{node_id, scid}, ...] in forward order
    amount_msat: int
    state: str = "proposed"  # proposed -> armed -> executing -> done/failed
    created: float = field(default_factory=time.time)


class EddyState:
    def __init__(self):
        self.waivers: dict[str, Waiver] = {}        # scid -> active waiver
        self.circulations: dict[str, Circulation] = {}
        self.gossip: dict[str, dict] = {}           # node_id -> {scid: {local,remote,cap}}

    def prune(self):
        now = time.time()
        for cid in list(self.circulations):
            c = self.circulations[cid]
            if c.state in ("done", "failed") and now - c.created > 300:
                del self.circulations[cid]


state = EddyState()


# ── helpers ────────────────────────────────────────────────────────────

def node_id() -> str:
    return plugin.rpc.getinfo()["id"]


def peer_node_ids() -> set[str]:
    return {p["id"] for p in plugin.rpc.listpeers()["peers"] if p.get("connected")}


def apply_waiver(scid: str) -> Optional[Waiver]:
    """Zero the channel's fees and record the restore point."""
    for ch in plugin.rpc.listpeerchannels()["channels"]:
        if ch.get("short_channel_id") == scid:
            fb, ppm = ch["fee_base_msat"], ch["fee_proportional_millionths"]
            if scid in state.waivers:
                # already waived under an earlier circulation: keep the
                # ORIGINAL restore point (capturing 0/0 here cascaded a
                # permanently-free channel — live-earned)
                w = state.waivers[scid]
                plugin.rpc.setchannel(scid, 0, 0)
                w.expires = time.time() + WAIVER_WINDOW_S
                return w
            plugin.rpc.setchannel(scid, 0, 0)
            w = Waiver(scid=scid, feebase=fb, feeppm=ppm,
                       expires=time.time() + WAIVER_WINDOW_S)
            state.waivers[scid] = w
            plugin.log(f"waiver armed: {scid} (feebase {fb}, ppm {ppm})")
            return w
    return None


def find_channel(scid: str) -> Optional[dict]:
    """v26 listpeerchannels has no 'active' key — NORMAL + connected peer
    is the live-channel predicate."""
    for ch in plugin.rpc.listpeerchannels()["channels"]:
        if (ch.get("short_channel_id") == scid
                and ch.get("state") == "CHANNELD_NORMAL"
                and ch.get("peer_connected")):
            return ch
    return None


def restore_waiver(w: Waiver) -> None:
    try:
        plugin.rpc.setchannel(w.scid, w.feebase, w.feeppm)
        plugin.log(f"waiver restored: {w.scid} "
                   f"(feebase {w.feebase}, ppm {w.feeppm})")
    except Exception as e:
        plugin.log(f"WAIVER RESTORE FAILED {w.scid}: {e} — manual "
                   f"restore needed: setchannel {w.scid} {w.feebase} {w.feeppm}",
                   level="error")
    state.waivers.pop(w.scid, None)


def restore_expired():
    now = time.time()
    for scid in list(state.waivers):
        w = state.waivers[scid]
        if now > w.expires:
            plugin.log(f"watchdog: restoring expired waiver {scid}")
            restore_waiver(w)


def beneficial(scid: str, amount_msat: int) -> bool:
    """A hop accepts if forwarding amount_msat OUT via scid moves the
    channel toward balance (local surplus on that side)."""
    ch = find_channel(scid)
    plugin.log(f"beneficial? scid={scid} found={bool(ch)}", level="info")
    if not ch:
        return False
    spendable = int(Millisatoshi(ch["spendable_msat"]).millisatoshis)
    total = int(ch["total_msat"])
    verdict = spendable - amount_msat > total / 2
    plugin.log(f"beneficial({scid}, {amount_msat}): spendable={spendable} "
               f"total={total} -> {verdict}", level="info")
    return verdict


def send_custommsg(peer: str, payload: dict) -> None:
    # CLN's sendcustommsg wants the FULL wire message hex INCLUDING the
    # 2-byte type prefix; hex(json) alone is an invalid even message that
    # connectd logs as "Invalid unknown even msg" (and can kill channeld)
    wire = format(EDDY_MSG_KIND, "04x") + json.dumps(payload).encode().hex()
    plugin.rpc.sendcustommsg(peer, wire)


# ── custommsg protocol ─────────────────────────────────────────────────

@plugin.hook("custommsg")
def on_custommsg(peer_id: str, payload: str, **kwargs):
    try:
        kind = int(payload[:4], 16)
        if kind != EDDY_MSG_KIND:
            return {"result": "continue"}
        msg = json.loads(bytes.fromhex(payload[4:]).decode())
    except Exception:
        return {"result": "continue"}  # not ours; never break peer traffic

    mtype = msg.get("t")
    if mtype == "propose":
        handle_propose(peer_id, msg)
    elif mtype == "exec":
        handle_exec(msg)
    elif mtype == "reject":
        handle_reject(msg)
    elif mtype == "ringcomplete":
        handle_ringcomplete(msg)
    return {"result": "continue"}


def handle_reject(msg: dict) -> None:
    cid = msg.get("cid", "")
    c = state.circulations.get(cid)
    if not c:
        return
    c.state = "rejected"
    plugin.log(f"circulation {cid[:8]}: rejected by {msg.get('node','?')[:12]}… "
               f"({msg.get('why','?')}) — restoring waivers")
    for h in c.hops:
        w = state.waivers.get(h.get("scid", ""))
        if w:
            restore_waiver(w)


def handle_ringcomplete(msg: dict) -> None:
    """All hops armed — fire the self-payment around the ring."""
    cid = msg["cid"]
    c = state.circulations.get(cid)
    if not c:
        return
    hops, amt = msg["hops"], msg["amt"]
    # hops = [{node: n1, scid: n1->n2}, {node: n2, scid: n2->n3}, ...] in
    # forward order. A sendpay route entry describes the hop TO that
    # destination VIA the previous node's channel: entry i uses
    # hops[i-1]["scid"] for i>0, and OUR channel for the first entry.
    route = []
    for i, h in enumerate(hops):
        channel = hops[i - 1]["scid"] if i > 0 else None
        if i == 0:
            continue  # we are the sender, not a route entry
        route.append({"id": h["node"],
                      "channel": channel,
                      "delay": 34 + 14 * (len(hops) - i),
                      "amount_msat": amt})
    # closing hop back to us rides the LAST node's channel
    route.append({"id": hops[0]["node"],
                  "channel": hops[-1]["scid"],
                  "delay": 34,
                  "amount_msat": amt})
    inv = plugin.rpc.invoice(amount_msat=amt, label=f"eddy-{cid}",
                             description=f"eddy circulation {cid[:8]}")
    amt_str = f"{amt}msat"
    for r in route:
        r["amount_msat"] = amt_str
        r["style"] = "tlv"
    try:
        plugin.rpc.sendpay(route, inv["payment_hash"],
                           label=f"eddy-{cid}",
                           payment_secret=inv["payment_secret"])
    except Exception as e:
        c.state = f"sendpay failed: {str(e)[:80]}"
        send_result(cid, "failed")
        plugin.log(f"circulation {cid[:8]}: SENDPAY FAILED — {str(e)[:200]}")
    else:
        deadline = time.time() + 60
        status = "pending"
        while time.time() < deadline:
            try:
                plugin.rpc.waitsendpay(inv["payment_hash"], timeout=10)
                status = "complete"
                break
            except Exception as e:
                msg = str(e)
                if "Timed out" not in msg and "timeout" not in msg.lower():
                    status = f"failed: {msg[:80]}"
                    break
                pay = plugin.rpc.listpays(inv["payment_hash"])
                pays = pay.get("pays", []) if isinstance(pay, dict) else []
                if pays and pays[-1].get("status") in ("complete", "failed"):
                    status = pays[-1]["status"]
                    break
        c.state = status
        send_result(cid, status)
        plugin.log(f"circulation {cid[:8]}: {status.upper()} — restoring waivers")
    for h in c.hops:
        w = state.waivers.get(h.get("scid", ""))
        if w:
            restore_waiver(w)


def send_result(cid: str, result: str) -> None:
    c = state.circulations.get(cid)
    if not c:
        return
    for h in c.hops[1:]:
        send_custommsg(h["node"], {"t": "exec", "cid": cid, "result": result})


def handle_propose(peer_from: str, msg: dict) -> None:
    """A PROPOSE arrives from the previous hop. We are hop N: check
    benefit, arm our waiver, append ourselves, forward to the next hop —
    or close the ring back to the initiator."""
    cid = msg["cid"]
    hops = msg["hops"]          # [{"node":..,"scid":..}, ...]
    amt = msg["amt"]
    me = node_id()
    plugin.log(f"PROPOSE received: cid={cid[:8]} hops={[(h['node'][:8], h['scid']) for h in hops]} next_scid={msg.get('next_scid')} amt={amt}")

    if any(h["node"] == me for h in hops):
        plugin.log(f"circulation {cid[:8]}: loop detected, dropping")
        return

    my_scid = msg.get("next_scid")
    if not my_scid:
        # initiator didn't pick our outgoing channel: choose our funded
        # channel toward a node NOT already on the ring (prefer the
        # initiator — closing the ring beats extending it)
        me = node_id()
        on_ring = {h["node"] for h in hops} | {me}
        init_id = hops[0]["node"] if hops else msg.get("initiator", "")
        best = None
        fallback = None
        others = on_ring - {init_id}  # the initiator's channel CLOSES the ring
        incoming = hops[-1]["scid"] if hops else None
        for ch in plugin.rpc.listpeerchannels()["channels"]:
            sc = ch.get("short_channel_id")
            if (ch.get("state") == "CHANNELD_NORMAL" and ch.get("peer_connected") and sc
                    and ch["peer_id"] not in others
                    and sc != incoming):  # never echo the channel we received on
                spendable = int(Millisatoshi(ch["spendable_msat"]).millisatoshis)
                total = int(ch["total_msat"])
                if spendable <= amt or spendable - amt <= total / 2:
                    continue  # funded enough AND still beneficial after
                if ch["peer_id"] == init_id:
                    best = sc  # closing the ring beats extending it
                    break
                fallback = fallback or sc
        my_scid = best or fallback
    if not my_scid or not beneficial(my_scid, amt):
        send_custommsg(peer_from, {"t": "reject", "cid": cid,
                                   "node": me,
                                   "why": "no channel" if not my_scid else "not beneficial"})
        plugin.log(f"circulation {cid[:8]}: rejected (not beneficial)")
        return

    w = apply_waiver(my_scid)
    if w is None:
        send_custommsg(peer_from, {"t": "reject", "cid": cid,
                                   "node": me, "why": "channel gone"})
        return

    hops.append({"node": me, "scid": my_scid})
    state.circulations[cid] = Circulation(cycle_id=cid, hops=hops,
                                          amount_msat=amt, state="armed")

    next_node = None
    for ch in plugin.rpc.listpeerchannels()["channels"]:
        if ch.get("short_channel_id") == my_scid and ch.get("peer_connected"):
            next_node = ch["peer_id"]
            break
    init_id = msg.get("initiator") or (hops[0]["node"] if hops else "")
    if next_node == init_id:
        send_custommsg(init_id, {"t": "ringcomplete", "cid": cid,
                                 "hops": hops, "amt": amt})
    elif next_node:
        send_custommsg(next_node, {
            "t": "propose", "cid": cid, "hops": hops, "amt": amt,
            "next_scid": None, "initiator": init_id})
    else:
        plugin.log(f"circulation {cid[:8]}: chosen channel {my_scid} has "
                   f"no connected peer — stuck", level="warn")


def hops_next_is_initiator(msg: dict) -> str:
    return msg.get("next_node", "")


def handle_exec(msg: dict) -> None:
    """The initiator fired and the payment settled (or failed): restore
    every waiver we hold for this circulation."""
    cid = msg["cid"]
    c = state.circulations.get(cid)
    if not c:
        return
    c.state = msg.get("result", "done")
    for h in c.hops:
        if h["node"] == node_id():
            w = state.waivers.get(h["scid"])
            if w:
                restore_waiver(w)
    plugin.log(f"circulation {cid[:8]}: {c.state}, waivers restored")


# ── T1: the operator RPC ───────────────────────────────────────────────

@plugin.method("eddy-rebalance")
def eddy_rebalance(scid: str, amount_sat: int, cycle_msat: Optional[int] = None):
    """On-demand circular rebalance through `scid` (T1 trigger).

    Builds the propose-chain from live channel state, arms waivers
    hop-by-hop, fires a self-payment around the ring, restores fees.
    """
    restore_expired()
    me = node_id()
    amt = amount_sat * 1000

    ch = find_channel(scid)
    if not ch:
        raise RpcException(f"unknown or inactive channel {scid}")

    a_id = ch["peer_id"]

    cid = os.urandom(8).hex()
    me_id = node_id()
    state.circulations[cid] = Circulation(
        cycle_id=cid, hops=[{"node": me_id, "scid": scid}], amount_msat=amt)
    # The initiator arms its OWN outgoing channel, then proposes to A.
    w = apply_waiver(scid)
    if not w:
        raise RpcException(f"could not arm waiver on {scid}")
    send_custommsg(a_id, {
        "t": "propose", "cid": cid, "amt": amt,
        "hops": [{"node": me, "scid": scid}],
        "next_scid": None,  # A picks its own outgoing channel
        "initiator": me})
    return {"cycle_id": cid, "status": "proposed",
            "waiver_window_s": WAIVER_WINDOW_S,
            "note": "watch eddy-status; fees restore on settle or watchdog"}


@plugin.method("eddy-status")
def eddy_status():
    restore_expired()
    return {
        "circulations": [{**vars(c).copy()} for c in state.circulations.values()],
        "active_waivers": [vars(w).copy() for w in state.waivers.values()],
    }


@plugin.method("eddy-restore-all")
def eddy_restore_all():
    """Manual safety: restore every active waiver to its recorded fees."""
    restored = []
    for scid in list(state.waivers):
        w = state.waivers[scid]
        restore_waiver(w)
        restored.append(scid)
    return {"restored": restored}


def _watchdog_loop():
    import threading
    while True:
        time.sleep(5)
        try:
            restore_expired()
        except Exception:
            pass


@plugin.init()
def init(options, configuration, plugin):
    threading.Thread(target=_watchdog_loop, daemon=True).start()
    plugin.log(f"eddy-cln ready on {node_id()[:12]}… "
               f"(waiver window {WAIVER_WINDOW_S}s, watchdog 5s, "
               f"custommsg kind {EDDY_MSG_KIND})")


plugin.run()
