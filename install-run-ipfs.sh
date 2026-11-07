#! /bin/sh
set -e
wget -q https://github.com/ipfs/kubo/releases/download/v0.20.0/kubo_v0.20.0_linux-amd64.tar.gz -O /tmp/kubo_linux-amd64.tar.gz
tar -xf /tmp/kubo_linux-amd64.tar.gz
export PATH=$PATH:$PWD/kubo/
ipfs init
ipfs daemon --routing=dhtserver &
for i in $(seq 1 60); do
    if curl -sf -X POST http://localhost:5001/api/v0/version > /dev/null; then
        exit 0
    fi
    sleep 1
done
echo "ipfs daemon did not start"
exit 1
