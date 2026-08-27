#!/bin/bash
# D2 phase 3: rotational skew to make all ring edges positive-delta, then capture circulation
LOG=/tmp/d2-phase3.log
LNCLI="/opt/lnd/lncli --network=signet --lnddir=/home/lnd/.lnd"

echo "[P3] $(date -u +%H:%M:%S) eddyd status:" >> $LOG
for n in l1 l2 l3; do
  C=$(docker top lsr-$n -o pid,comm 2>/dev/null | awk '$2=="eddyd"' | wc -l)
  echo "  $n eddyd procs: $C" >> $LOG
done

deltas() {
  for n in l1 l2 l3; do
    echo "  --- $n:" >> $LOG
    docker exec lsr-$n $LNCLI listchannels 2>/dev/null | python3 -c "
import json,sys
for c in json.load(sys.stdin).get('channels',[]):
    cap=int(c.get('capacity',0)); loc=int(c.get('local_balance',0)); rem=int(c.get('remote_balance',0))
    print('    chan=%s peer=%s local=%d remote=%d delta=%+d' % (c.get('chan_id'), c.get('peer_alias','?'), loc, rem, loc-cap//2))" >> $LOG 2>&1
  done
}

echo "[P3] BEFORE skew:" >> $LOG
deltas

echo "[P3] rotational skew (l2->l1, l3->l2, l1->l3, 5000 each):" >> $LOG
INV1=$(docker exec lsr-l1 $LNCLI addinvoice --amt=5000 --memo="rot-1" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('payment_request',''))")
docker exec lsr-l2 $LNCLI payinvoice --force --fee_limit=1000 "$INV1" >> $LOG 2>&1
echo "  l2->l1 rc=$?" >> $LOG
INV2=$(docker exec lsr-l2 $LNCLI addinvoice --amt=5000 --memo="rot-2" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('payment_request',''))")
docker exec lsr-l3 $LNCLI payinvoice --force --fee_limit=1000 "$INV2" >> $LOG 2>&1
echo "  l3->l2 rc=$?" >> $LOG
INV3=$(docker exec lsr-l3 $LNCLI addinvoice --amt=5000 --memo="rot-3" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('payment_request',''))")
docker exec lsr-l1 $LNCLI payinvoice --force --fee_limit=1000 "$INV3" >> $LOG 2>&1
echo "  l1->l3 rc=$?" >> $LOG

echo "[P3] AFTER skew (expect rotational positive deltas):" >> $LOG
deltas

echo "[P3] waiting for gossip+solver+circulation (240s)..." >> $LOG
sleep 240

echo "[P3] $(date -u +%H:%M:%S) EVIDENCE:" >> $LOG
for n in l1 l2 l3; do
  echo "=== $n eddy.log tail:" >> $LOG
  docker exec lsr-$n sh -c 'tail -50 /root/.eddy/eddy.log' >> $LOG 2>&1
  echo "=== $n fwdinghistory:" >> $LOG
  docker exec lsr-$n $LNCLI fwdinghistory --start_time $(date -d "20 minutes ago" +%s) >> $LOG 2>&1
done

echo "[P3] FINAL deltas:" >> $LOG
deltas
echo "[P3] DONE $(date -u +%H:%M:%S)" >> $LOG
