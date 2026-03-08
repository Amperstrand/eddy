#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== Eddy Demo ==="

# Extract Polar network if not already done
if [ ! -d network ]; then
    if [ ! -f demo-network.polar.zip ]; then
        echo "Error: demo-network.polar.zip not found"
        exit 1
    fi
    echo "Extracting network..."
    mkdir network && cd network && unzip ../demo-network.polar.zip && cd ..
fi

# Start docker network
echo "Starting docker containers..."
cd network && docker compose up -d && cd ..

# Wait for LND nodes to be ready (check that gRPC port accepts connections)
echo "Waiting for LND nodes..."
for port in 10001 10002 10003 10004; do
    printf "  Waiting for localhost:%s..." "$port"
    for i in $(seq 1 90); do
        if nc -z localhost "$port" 2>/dev/null; then
            # Port is open, but LND may still be initializing — wait for wallet to be ready
            # by checking if the TLS cert exists and the node responds to a basic gRPC call
            sleep 2
            echo " ready"
            break
        fi
        if [ "$i" -eq 90 ]; then
            echo " timeout!"
            echo "Node on port $port did not become ready"
            exit 1
        fi
        sleep 1
    done
done
# Give nodes a moment to finish unlocking wallets
echo "  Waiting for wallets to unlock..."
sleep 5

LNCLI="/opt/lnd/lncli"
LND_ARGS="--rpcserver=localhost:10009 --network=regtest --lnddir=/home/lnd/.lnd"

# Mine blocks to keep the regtest chain current (LND won't sync if chain time is stale)
echo "Mining initial blocks..."
MINING_ADDR=$(docker exec polar-n1-backend2 bitcoin-cli -regtest -rpcuser=polaruser -rpcpassword=polarpass getnewaddress)
docker exec polar-n1-backend2 bitcoin-cli -regtest -rpcuser=polaruser -rpcpassword=polarpass generatetoaddress 6 "$MINING_ADDR" > /dev/null

# Wait for LND nodes to sync to chain
echo "  Waiting for chain sync..."
for node in alice bob carol dave; do
    for i in $(seq 1 30); do
        if docker exec polar-n1-$node $LNCLI $LND_ARGS getinfo 2>/dev/null | grep -q 'synced_to_chain.*true'; then
            break
        fi
        sleep 1
    done
done
echo "  Nodes synced"

# Start background block miner (1 block every 10 seconds to keep chain alive)
echo "Starting background block miner..."
(while true; do
    docker exec polar-n1-backend2 bitcoin-cli -regtest -rpcuser=polaruser -rpcpassword=polarpass generatetoaddress 1 "$MINING_ADDR" > /dev/null 2>&1
    sleep 1
done) &
MINER_PID=$!

# Reconnect peers (Polar exports lose peer connections on restart)
echo "Reconnecting peers..."
ALICE_PUB=$(docker exec polar-n1-alice $LNCLI $LND_ARGS getinfo 2>/dev/null | grep identity_pubkey | cut -d'"' -f4)
BOB_PUB=$(docker exec polar-n1-bob $LNCLI $LND_ARGS getinfo 2>/dev/null | grep identity_pubkey | cut -d'"' -f4)
CAROL_PUB=$(docker exec polar-n1-carol $LNCLI $LND_ARGS getinfo 2>/dev/null | grep identity_pubkey | cut -d'"' -f4)
DAVE_PUB=$(docker exec polar-n1-dave $LNCLI $LND_ARGS getinfo 2>/dev/null | grep identity_pubkey | cut -d'"' -f4)
docker exec polar-n1-alice $LNCLI $LND_ARGS connect ${BOB_PUB}@polar-n1-bob:9735 > /dev/null 2>&1 || true
docker exec polar-n1-alice $LNCLI $LND_ARGS connect ${CAROL_PUB}@polar-n1-carol:9735 > /dev/null 2>&1 || true
docker exec polar-n1-alice $LNCLI $LND_ARGS connect ${DAVE_PUB}@polar-n1-dave:9735 > /dev/null 2>&1 || true
docker exec polar-n1-bob $LNCLI $LND_ARGS connect ${CAROL_PUB}@polar-n1-carol:9735 > /dev/null 2>&1 || true
docker exec polar-n1-bob $LNCLI $LND_ARGS connect ${DAVE_PUB}@polar-n1-dave:9735 > /dev/null 2>&1 || true
docker exec polar-n1-carol $LNCLI $LND_ARGS connect ${DAVE_PUB}@polar-n1-dave:9735 > /dev/null 2>&1 || true
echo "  Peers connected"

# Build eddyd
echo "Building eddyd..."
(cd .. && go build -o eddy-demo/eddyd ./cmd/eddyd/)
EDDYD_FLAG="--eddyd ./eddyd"

cleanup() {
    echo ""
    echo "Tearing down regtest network..."
    kill $MINER_PID 2>/dev/null || true
    (cd network && docker compose down -v)
    rm -rf network eddyd eddyd-*.log eddyd-*.yaml
    echo "Done."
}
trap cleanup EXIT

# Run the TUI
echo "Launching TUI..."
go run . --network ./network $EDDYD_FLAG
