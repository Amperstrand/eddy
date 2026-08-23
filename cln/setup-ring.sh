#!/usr/bin/env bash
# Fund wallets + open the 3-node ring: n1-n2, n2-n3, n3-n1 (1M sat each).
# Idempotent: skips steps already complete.
set -euo pipefail
cd "$(dirname "$0")/.."

BC="docker exec eddy-bitcoind bitcoin-cli -regtest -rpcuser=user -rpcpassword=pass -rpcwallet=default"
lc() { docker exec "eddy-$1" lightning-cli --regtest "${@:2}"; }
mine() { $BC generatetoaddress "$1" "$FAUCET" >/dev/null; }

docker exec eddy-bitcoind bitcoin-cli -regtest -rpcuser=user -rpcpassword=pass \
  createwallet default >/dev/null 2>&1 || true

echo "[ring] waiting for nodes…"
for n in n1 n2 n3; do
  for i in $(seq 1 60); do lc "$n" getinfo >/dev/null 2>&1 && break; sleep 1; done
done
FAUCET=$(lc n1 newaddr bech32 | jq -r .bech32)
$BC generatetoaddress 101 "$FAUCET" >/dev/null 2>&1 || true
sleep 3
mine 1
echo "[ring] funding n2, n3 from n1 (mined 101 to n1)"
A2=$(lc n2 newaddr bech32 | jq -r .bech32)
A3=$(lc n3 newaddr bech32 | jq -r .bech32)
lc n1 withdraw "$A2" 4000000 >/dev/null
lc n1 withdraw "$A3" 4000000 >/dev/null
mine 2

echo "[ring] connecting peers"
P1=$(lc n1 getinfo | jq -r .id); P2=$(lc n2 getinfo | jq -r .id); P3=$(lc n3 getinfo | jq -r .id)
lc n1 connect "$P2@eddy-n2" >/dev/null 2>&1 || true
lc n2 connect "$P3@eddy-n3" >/dev/null 2>&1 || true
lc n3 connect "$P1@eddy-n1" >/dev/null 2>&1 || true

echo "[ring] opening channels n1->n2, n2->n3, n3->n1"
chans() { lc "$1" listpeerchannels | jq -r '.channels[]?.short_channel_id' | grep -v '^$' | wc -l; }
if [ "$(chans n1)" -lt 1 ]; then lc n1 fundchannel "$P2" 1000000 >/dev/null; fi
if [ "$(chans n2)" -lt 1 ]; then lc n2 fundchannel "$P3" 1000000 >/dev/null; fi
if [ "$(chans n3)" -lt 1 ]; then lc n3 fundchannel "$P1" 1000000 >/dev/null; fi
mine 6
sleep 5

echo "[ring] done:"
for n in n1 n2 n3; do
  echo "  $n: $(chans $n) channel(s), $(lc $n listfunds | jq -r '.outputs | length') utxos"
done
echo "[ring] plugin check:"
lc n1 plugin list 2>/dev/null | jq -r '.plugins[].name' 2>/dev/null | grep eddy || echo "  (eddy_cln.py not loaded — check plugin-dir)"
