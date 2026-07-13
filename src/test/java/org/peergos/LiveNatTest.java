package org.peergos;

import io.ipfs.multiaddr.*;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.config.*;
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.stream.*;

/**
 * Live shakeout against the public IPFS DHT: boot a full node (AutoNAT + Circuit Relay v2 + DCUtR +
 * UPnP all enabled), bootstrap, and observe reachability detection, discovered/announced addresses and
 * relay reservations over a couple of minutes. Not a CI test - it hits the real network and is timing
 * dependent; run explicitly with -Dtest=LiveNatTest.
 */
public class LiveNatTest {

    @Test
    @Ignore // manual: hits the public network and runs for ~2 minutes; run with -Dtest=LiveNatTest
    public void exerciseOnRealDht() throws Exception {
        int port = 4001 + new Random().nextInt(20000);
        List<MultiAddress> swarm = List.of(
                new MultiAddress("/ip4/0.0.0.0/tcp/" + port),
                new MultiAddress("/ip4/0.0.0.0/udp/" + port + "/quic-v1"));

        HostBuilder builder = new HostBuilder(new RamAddressBook()).generateIdentity();
        PrivKey priv = builder.getPrivateKey();
        IdentitySection id = new IdentitySection(priv.bytes(), builder.getPeerId());

        EmbeddedPeer node = EmbeddedPeer.build(new RamRecordStore(), swarm,
                Config.defaultBootstrapNodes, id, Collections.emptyList(), new RamAddressBook(), Optional.empty());
        node.enablePortForwarding();
        System.out.println("=== LiveNatTest peerId=" + node.node.getPeerId() + " port=" + port);
        node.start(true);

        long total = 110_000;
        long deadline = System.currentTimeMillis() + total;
        ReachabilityManager reach = node.getReachability();
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(10_000);
            int connections = node.node.getNetwork().getConnections().size();
            List<Multiaddr> listen = node.node.listenAddresses();
            List<Multiaddr> relayed = listen.stream()
                    .filter(a -> a.toString().contains("p2p-circuit")).collect(Collectors.toList());
            System.out.println(String.format(
                    "[t+%3ds] conns=%d reachability=%s observed=%s confirmedPublic=%s relayedAddrs=%s",
                    (total - (deadline - System.currentTimeMillis())) / 1000,
                    connections,
                    reach.getReachability(),
                    reach.getAllObservedAddresses(),
                    reach.getConfirmedPublicAddresses(),
                    relayed));
        }
        System.out.println("=== Final listen addresses: " + node.node.listenAddresses());
        node.stop().join();
    }
}
