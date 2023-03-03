# Nabu

A minimal Java implementation of [IPFS](https:/ipfs.io)

[Nabu](https://en.wikipedia.org/wiki/Nabu) is the ancient Mesopotamian patron god of literacy, the rational arts, scribes, and wisdom.

## Status
This is a WIP. You can follow our progress updates [here](https://peergos.net/public/ianopolous/work/java-ipfs-updates.md?open=true).

Currently implemented properties:
* TCP transport
* Noise encryption and authentication
* TLS security provider (with early muxer negotiation using ALPN)
* RSA and Ed25519 peer IDs
* yamux and mplex muxers
* Kademlia DHT for content discovery
* Bitswap 1.2.0
* IPNS publishing on Kademlia
* p2p http proxy
* dnsaddr multiaddr resolution during bootstrap
* autonat
* uPnP port forwarding
* nat-pmp port forwarding
* file based blockstore
* persistent datastores (IPNS record store) using H2 DB
* persistent identities and config

In the future we will add:
* circuit-relay
* dcutr (direct connection upgrade through relay)
* AutoRelay
* block HTTP API
* QUIC transport (and encryption and multiplexing)
* mDNS peer discovery
* S3 blockstores
* bloom/infinifilter filtered blockstore
* Android compatibility

## Usage
You can use this as a standalone application for storing and retrieving blocks. Or you can embed it in your application. 

### Maven, Gradle, SBT

Package managers are supported through [JitPack](https://jitpack.io/#ipfs/java-ipfs-http-client/) which supports Maven, Gradle, SBT, etc.

for Maven, add the following sections to your pom.xml (replacing $LATEST_VERSION):
```
  <repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
  </repositories>

  <dependencies>
    <dependency>
      <groupId>com.github.peergos</groupId>
      <artifactId>nabu</artifactId>
      <version>$LATEST_VERSION</version>
    </dependency>
  </dependencies>
```
