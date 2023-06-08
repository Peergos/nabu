#! /bin/sh
wget https://dist.ipfs.io/kubo/v0.20.0/kubo_v0.20.0_linux-amd64.tar.gz -O /tmp/kubo_linux-amd64.tar.gz
tar -xvf /tmp/kubo_linux-amd64.tar.gz
export PATH=$PATH:$PWD/kubo/
ipfs init
ipfs daemon --routing=dhtserver &
sleep 10s
time ipfs pin add zdpuAwfJrGYtiGFDcSV3rDpaUrqCtQZRxMjdC6Eq9PNqLqTGg
