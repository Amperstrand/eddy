#!/bin/bash
# D2 phase 2: restart eddyd with small min_push, triangulate imbalance, capture circulation
LOG=/tmp/d2-phase2.log
LNCLI="/opt/lnd/lncli --network=signet --lnddir=/home/lnd/.lnd"
CFG=/tmp/eddy-config.yaml

cat > $CFG << 'EOF'
log_level: debug
data_dir: /root/.eddy
solver:
  max_cycle_length: 5
  stale_threshold: 90
  min_push_amount: 500
circulation:
  timeout: 30s
  payment_timeout: 60s
gossip:
  interval: 10s
EOF

echo "[P2] $(date -u +%H:%M:%S) killing old eddyd (host pids via docker top)..." >> $LOG
for n in l1 l2 l3; do
  for pid in $(docker top lsr-$n -o pid,comm 2>/dev/null | awk '$2=="eddyd"{print $1}'); do
    kill $pid 2>/dev/null && echo "  $n killed host-pid $pid" >> $LOG
  done
done
sleep 2

echo "[P2] starting eddyd with config..." >> $LOG
for n in l1 l2 l3; do
  docker cp $CFG lsr-$n:/root/config.yaml >> $LOG 2>&1
  docker exec -d lsr-$n sh -c '/home/lnd/eddyd \
    --lnd-host localhost:10009 \
    --lnd-cert /home/lnd/.lnd/tls.cert \
    --lnd-macaroon /home/lnd/.lnd/data/chain/bitcoin/signet/admin.macaroon \
    --config /root/config.yaml'
done
sleep 5
for n in l1 l2 l3; do
  C=$(docker top lsr-$n -o pid,comm 2>/dev/null | awk '$2=="eddyd"' | wc -l)
  echo "  $n eddyd procs: $C" >> $LOG
  docker exec lsr-$n sh -c 'tail -3 /root/.eddy/eddy.log' >> $LOG 2>&1
done

echo "[P2] triangulating imbalance:" >> $LOG
# 1) routed payment l1 -> (via l2) -> l3: makes l1-l2 l1-light AND l2-l3 l2-light
INV3=$(docker exec lsr-l3 $LNCLI addinvoice --amt=5000 --memo="skew-routed" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('payment_request',''))")
echo "  l3 invoice: ${INV3:0:24}..." >> $LOG
docker exec lsr-l1 $LNCLI payinvoice --force --fee_limit=1000 "$INV3" >> $LOG 2>&1
echo "  routed pay rc=$?" >> $LOG
# 2) direct payment l3 -> l1: makes l3-l1 l3-light
INV1=$(docker exec lsr-l1 $LNCLI addinvoice --amt=5000 --memo="skew-direct" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('payment_request',''))")
echo "  l1 invoice: ${INV1:0:24}..." >> $LOG
docker exec lsr-l3 $LNCLI payinvoice --force --fee_limit=1000 "$INV1" >> $LOG 2>&1
echo "  direct pay rc=$?" >> $LOG

echo "[P2] post-skew deltas:" >> $LOG
for n in l1 l2 l3; do
  echo "  --- $n:" >> $LOG
  docker exec lsr-$n $LNCLI listchannels 2>/dev/null | python3 -c "
import json,sys
for c in json.load(sys.stdin).get('channels',[]):
    cap=c.get('capacity'); loc=c.get('local_balance')
    print('    chan=%s local=%d remote=%d delta=%+d' % (c.get('chan_id'), loc, c.get('remote_balance'), loc-cap//2))" >> $LOG 2>&1
done

echo "[P2] waiting for gossip+solver (240s)..." >> $LOG
sleep 240

echo "[P2] $(date -u +%H:%M:%S) EVIDENCE CAPTURE" >> $LOG
for n in l1 l2 l3; do
  echo "=== $n eddy.log (solver/circulation lines):" >> $LOG
  docker exec lsr-$n sh -c 'grep -E "solver|circulat|proposal|PROPPOSE|execute|settle|interceptor" /root/.eddy/eddy.log | tail -40' >> $LOG 2>&1
  echo "=== $n fwdinghistory:" >> $LOG
  docker exec lsr-$n $LNCLI fwdinghistory --start_time $(date -d "15 minutes ago" +%s) >> $LOG 2>&1
done

echo "[P2] final deltas:" >> $LOG
for n in l1 l2 l3; do
  echo "  --- $n:" >> $LOG
  docker exec lsr-$n $LNCLI listchannels 2>/dev/null | python3 -c "
import json,sys
for c in json.load(sys.stdin).get('channels',[]):
    cap=c.get('capacity'); loc=c.get('local_balance')
    print('    chan=%s local=%d remote=%d delta=%+d' % (c.get('chan_id'), loc, c.get('remote_balance'), loc-cap//2))" >> $LOG 2>&1
done
echo "[P2] DONE $(date -u +%H:%M:%S)" >> $LOG
