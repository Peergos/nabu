package org.peergos;

import com.google.protobuf.*;
import identify.pb.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multiformats.Protocol;
import io.libp2p.multistream.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.*;
import org.peergos.protocol.circuit.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

public class RelayTest {

    public static List<RelayTransport.CandidateRelay> findRelayedPeer(Kademlia dht, Host us) {
        for (int i=0; i < 20; i++) {
            byte[] hash = new byte[32];
            new Random().nextBytes(hash);

            List<PeerAddresses> nodes = dht.findClosestPeers(new Multihash(Multihash.Type.sha2_256, hash), 20, us);
            List<RelayTransport.CandidateRelay> relayed = nodes.stream()
                    .filter(p -> !p.getPublicAddresses().isEmpty() && isRelayed(p))
                    .map(p -> new RelayTransport.CandidateRelay(PeerId.fromBase58(p.peerId.toBase58()),
                            p.addresses.stream()
                                    .filter(a -> a.has(Protocol.P2PCIRCUIT) &&
                                            !a.has(Protocol.QUIC) &&
                                            !a.has(Protocol.QUICV1))
                                    .collect(Collectors.toList())))
                    .collect(Collectors.toList());
            if (!relayed.isEmpty())
                return relayed;
        }
        throw new IllegalStateException("Couldn't find relay");
    }

    public static boolean isRelayed(PeerAddresses p) {
        try {
            return p.addresses.stream()
                    .anyMatch(a -> a.has(Protocol.P2PCIRCUIT));
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    public void pingRemoteRelayedPeer() {
        HostBuilder builder1 = HostBuilder.create(10000 + new Random().nextInt(50000),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(),
                (c, p, a) -> CompletableFuture.completedFuture(true)).enableRelay();
        Host node1 = builder1.build();
        node1.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1);

        try {
            bootstrapNode(builder1, node1);
            List<RelayTransport.CandidateRelay> relayed = findRelayedPeer(builder1.getWanDht().get(), node1);
            RelayTransport.CandidateRelay target = relayed.get(0);

            // connect to relayed peer from node1 via a relay
            Multiaddr node2ViaRelay = target.addrs.get(0).concatenated(Multiaddr.fromString("/p2p/" + target.id.toBase58()));
            System.out.println("Contacting " + node2ViaRelay);
            StreamPromise<? extends PingController> dial = new Ping().dial(node1, node2ViaRelay);
            PingController ping = dial.getController().join();
            for (int i=0; i < 10; i++)
                System.out.println("Relayed ping took " + ping.ping().join() + "ms");
        } finally {
            node1.stop();
        }
    }

    @Test
    public void remoteRelayToLocalPeer() {
        HostBuilder builder1 = HostBuilder.create(10000 + new Random().nextInt(50000),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(),
                (c, p, a) -> CompletableFuture.completedFuture(true)).enableRelay();
        Host node1 = builder1.build();
        node1.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1);

        HostBuilder builder2 = HostBuilder.create(10000 + new Random().nextInt(50000),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(),
                (c, p, a) -> CompletableFuture.completedFuture(true)).enableRelay();
        Host node2 = builder2.build();
        node2.start().join();
        IdentifyBuilder.addIdentifyProtocol(node2);

        try {
            bootstrapNode(builder1, node1);
            bootstrapNode(builder2, node2);

            // set up node 2 to listen via a relay
            List<RelayTransport.CandidateRelay> relays = RelayDiscovery.findRelay(builder2.getWanDht().get(), node2);
            RelayTransport.CandidateRelay relay = relays.get(0);
            Multiaddr relayAddr = Multiaddr.fromString(relay.addrs.stream()
                            .filter(a -> !a.toString().contains("/quic"))
                            .findFirst().get().toString())
                    .withP2P(relay.id);
            CircuitHopProtocol.HopController hop = builder2.getRelayHop().get().dial(node2, relayAddr).getController().join();
            CircuitHopProtocol.Reservation reservation = hop.reserve().join();

            // connect to node2 from node1 via a relay
            System.out.println("Using relay " + relayAddr);
            Multiaddr node2ViaRelay = relayAddr.concatenated(Multiaddr.fromString("/p2p-circuit/p2p/" + node2.getPeerId().toBase58()));
            StreamPromise<? extends PingController> dial = new Ping().dial(node1, node2ViaRelay);
            PingController ping = dial.getController().join();
            for (int i=0; i < 10; i++)
                System.out.println("Relayed ping took " + ping.ping().join() + "ms");
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void localRelay() {
        HostBuilder builder1 = HostBuilder.create(10000 + new Random().nextInt(50000),
                new RamProviderStore(10_000), new RamRecordStore(), new RamBlockstore(),
                (c, p, a) -> CompletableFuture.completedFuture(true))
                .enableRelay();
        Host sender = builder1.build();
        sender.start().join();
        IdentifyBuilder.addIdentifyProtocol(sender);

        HostBuilder builder2 = HostBuilder.create(10000 + new Random().nextInt(50000),
                new RamProviderStore(10_000), new RamRecordStore(), new RamBlockstore(),
                (c, p, a) -> CompletableFuture.completedFuture(true))
                .enableRelay();
        Host receiver = builder2.build();
        receiver.start().join();
        IdentifyBuilder.addIdentifyProtocol(receiver);

        HostBuilder relayBuilder = HostBuilder.create(10000 + new Random().nextInt(50000),
                new RamProviderStore(10_000), new RamRecordStore(), new RamBlockstore(),
                (c, p, a) -> CompletableFuture.completedFuture(true))
                .enableRelay();
        Host relay = relayBuilder.build();
        relay.start().join();
        IdentifyBuilder.addIdentifyProtocol(relay);

        try {
            // set up node 2 to listen via a relay
            Multiaddr relayAddr = relay.listenAddresses().get(0).withP2P(relay.getPeerId());
            CircuitHopProtocol.HopController hop = builder2.getRelayHop().get()
                    .dial(receiver, relayAddr).getController().join();
            CircuitHopProtocol.Reservation reservation = hop.reserve().join();

            // connect to node2 from node1 via a relay
            System.out.println("Using relay " + relay.getPeerId());
            Multiaddr node2ViaRelay = relayAddr.concatenated(Multiaddr.fromString("/p2p-circuit/p2p/" + receiver.getPeerId().toBase58()));
            PingController ping = new Ping().dial(sender, node2ViaRelay).getController().join();
            for (int i=0; i < 10; i++)
                System.out.println("Relayed ping took " + ping.ping().join() + "ms");
        } finally {
            sender.stop();
            receiver.stop();
            relay.stop();
        }
    }

    private static void bootstrapNode(HostBuilder builder, Host host) {
        // Don't connect to local kubo
        List<MultiAddress> bootStrapNodes = List.of(
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
                        "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ", // mars.i.ipfs.io
                        "/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
                        "/ip4/104.236.179.241/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
                        "/ip4/128.199.219.111/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
                        "/ip4/104.236.76.40/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
                        "/ip4/178.62.158.247/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
                        "/ip6/2604:a880:1:20:0:0:203:d001/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
                        "/ip6/2400:6180:0:d0:0:0:151:6001/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
                        "/ip6/2604:a880:800:10:0:0:4a:5001/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
                        "/ip6/2a03:b0c0:0:1010:0:0:23:1001/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd"
                ).stream()
                .map(MultiAddress::new)
                .collect(Collectors.toList());
        Kademlia dht = builder.getWanDht().get();
        Predicate<String> bootstrapAddrFilter = addr -> !addr.contains("/wss/"); // jvm-libp2p can't parse /wss addrs
        int connections = dht.bootstrapRoutingTable(host, bootStrapNodes, bootstrapAddrFilter);
        if (connections == 0)
            throw new IllegalStateException("No connected peers!");
        dht.bootstrap(host);
    }
}
