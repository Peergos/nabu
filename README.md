# Nabu

A minimal Java implementation of [IPFS](https:/ipfs.io)

[Nabu](https://en.wikipedia.org/wiki/Nabu) is the ancient Mesopotamian patron god of literacy, the rational arts, scribes, and wisdom.

## Status
This is a WIP.

Currently implemented properties:
* TCP transport
* Noise encryption and authentication
* RSA and Ed25519 peer IDs
* mplex
* Kademlia DHT for content discovery
* Bitswap 1.2.0
* IPNS publishing on Kademlia
* p2p http proxy
* dnsaddr multiaddr resolution during bootstrap

In the Future we will add:
* autonat
* circuit-relay
* uPnP
* nat-pmp
* dcutr (direct connection upgrade through relay)
* QUIC transport (and encryption and multiplexing)
* S3 blockstores

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