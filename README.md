# Nabu

A minimal Java implementation of [IPFS](https://ipfs.io)

[Nabu](https://en.wikipedia.org/wiki/Nabu) is the ancient Mesopotamian patron god of literacy, the rational arts, scribes, and wisdom.

## Status
This is a WIP. You can follow our progress updates [here](https://peergos.net/public/ianopolous/work/java-ipfs-updates.md?open=true).

Currently implemented properties:
* TCP transport
* Noise encryption and authentication
* TLS security provider (with early muxer negotiation using ALPN)
* RSA and Ed25519 peer IDs
* yamux and mplex muxers
* Kademlia DHT for peer routing, IPNS publishing and fallback content discovery
* Bitswap 1.2.0 + auth extension as [used in Peergos](https://peergos.org/posts/bats)
* p2p http proxy
* dnsaddr multiaddr resolution during bootstrap
* autonat
* uPnP port forwarding
* nat-pmp port forwarding
* file based blockstore
* persistent datastores (IPNS record store) using H2 DB
* persistent identities and config
* basic HTTP API (block.{get, put, rm, has, stat}, id, getRefs, bloomAdd) compatible with matching kubo api calls
* bloom/[infini filtered](https://www.rasmuspagh.net/papers/infinifilter.pdf) blockstore
* connect bitswap to kademlia for discovery, with a faster version with supplied peerids

In the future we will add:
* QUIC transport (and encryption and multiplexing)
* more interop tests with other languages
* configurable cid publishing function
* circuit-relay
* dcutr (direct connection upgrade through relay)
* AutoRelay
* S3 blockstore
* mDNS peer discovery
* Android compatibility
* example serverless chat app using p2p http proxy for Android and iOS

## Usage
You can use this as a standalone application for storing and retrieving blocks. Or you can embed it in your application. 

### Maven, Gradle, SBT

Package managers are supported through [JitPack](https://jitpack.io/#Peergos/nabu) which supports Maven, Gradle, SBT, etc.

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
