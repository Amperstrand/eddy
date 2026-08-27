#!/bin/bash
# D2 resume: skew l1->l2, start eddyd on all 3 nodes, capture evidence
LOG=/tmp/d2-resume.log
LNCLI="/opt/lnd/lncli --network=signet --lnddir=/home/lnd/.lnd"

echo "[D2r] $(date -u +%H:%M:%S) skewing (drain l1->l2 15k sats)..." >> $LOG
INV=$(docker exec lsr-l2 $LNCLI addinvoice --amt=15000 --memo="skew2" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('payment_request',''))")
if [ -z "$INV" ]; then
  echo "[D2r] FATAL: no invoice from l2" >> $LOG
  exit 1
fi
echo "[D2r] invoice ok (${INV:0:32}...)" >> $LOG
docker exec lsr-l1 $LNCLI payinvoice --force --fee_limit=100000 "$INV" >> $LOG 2>&1
PAY_RC=$?
echo "[D2r] payinvoice rc=$PAY_RC" >> $LOG

echo "[D2r] post-skew channel states:" >> $LOG
for n in l1 l2 l3; do
  docker exec lsr-$n $LNCLI listchannels 2>/dev/null | python3 -c "
import json,sys
for c in json.load(sys.stdin).get('channels',[]):
    print('  %s local=%s remote=%s active=%s' % ('chan', c.get('local_balance'), c.get('remote_balance'), c.get('active')))" >> $LOG 2>&1
done

echo "[D2r] $(date -u +%H:%M:%S) starting eddyd on all 3 nodes..." >> $LOG
for n in l1 l2 l3; do
  docker exec -d lsr-$n sh -c '/home/lnd/eddyd \
    --lnd-host localhost:10009 \
    --lnd-cert /home/lnd/.lnd/tls.cert \
    --lnd-macaroon /home/lnd/.lnd/data/chain/bitcoin/signet/admin.macaroon \
    --config /dev/null > /tmp/eddyd.log 2>&1'
done
sleep 3
for n in l1 l2 l3; do
  echo "$n eddyd pid: $(docker exec lsr-$n pgrep -x eddyd 2>/dev/null || echo NONE)" >> $LOG
  docker exec lsr-$n head -20 /tmp/eddyd.log 2>/dev/null >> $LOG
done

echo "[D2r] waiting for solver cycles (scans every 30s, waiting 180s)..." >> $LOG
sleep 180

echo "[D2r] $(date -u +%H:%M:%S) checking for circulations..." >> $LOG
START=$(date -d "10 minutes ago" +%s)
for n in l1 l2 l3; do
  echo "--- $n fwdinghistory:" >> $LOG
  docker exec lsr-$n $LNCLI fwdinghistory --start_time $START >> $LOG 2>&1
  echo "--- $n eddyd log tail:" >> $LOG
  docker exec lsr-$n tail -30 /tmp/eddyd.log 2>/dev/null >> $LOG
done

echo "[D2r] final channel states:" >> $LOG
for n in l1 l2 l3; do
  echo "--- $n:" >> $LOG
  docker exec lsr-$n $LNCLI listchannels >> $LOG 2>&1
done

echo "[D2r] payments during window:" >> $LOG
for n in l1 l2 l3; do
  echo "--- $n listpayments (in-flight/settled last):" >> $LOG
  docker exec lsr-$n $LNCLI listpayments >> $LOG 2>&1
done
echo "[D2r] DONE $(date -u +%H:%M:%S)" >> $LOG
