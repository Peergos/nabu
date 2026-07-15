package org.peergos;

import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.config.*;
import org.peergos.protocol.IdentifyBuilder;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Live DCUtR against the public IPFS DHT: find a peer that is only reachable through a relay
 * ({@code /p2p-circuit}), dial it over the relay, wait for the relayed peer to hole-punch us up to a
 * direct connection via DCUtR, and ping it over that direct connection.
 *
 * Manual and best-effort: it depends on finding a relay-only DCUtR-capable peer whose relay reservation
 * is still live, so it may not always succeed. Run explicitly with -Dtest=DcutrLiveTest.
 */
public class DcutrLiveTest {

    @Test
    @Ignore // manual: hits the public network; run with -Dtest=DcutrLiveTest
    public void holePunchARelayedPeer() throws Exception {
        int port = 4001 + new Random().nextInt(20000);
        BlockRequestAuthoriser authoriser = (c, p, a) -> CompletableFuture.completedFuture(true);
        HostBuilder builder = HostBuilder.create(port, new RamProviderStore(1000), new RamRecordStore(),
                new RamBlockstore(), authoriser);
        CompletableFuture<Connection> upgraded = new CompletableFuture<>();
        builder.enableDcutr(upgraded::complete); // replace the default no-op DCUtR callback
        Host node = builder.build();
        Kademlia dht = builder.getWanDht().get();

        node.start().join();
        IdentifyBuilder.addIdentifyProtocol(node, Collections.emptyList()); // register /ipfs/id/1.0.0
        System.out.println("=== DcutrLiveTest peerId=" + node.getPeerId() + " port=" + port);
        try {
            dht.bootstrapRoutingTable(node, Config.defaultBootstrapNodes, addr -> ! addr.contains("/wss/"));
            dht.bootstrap(node);
            dht.startBootstrapThread(node);

            // find relay-reachable peers and dial the first one whose relay reservation is still live
            // (DHT-advertised relay addresses often go stale -> NO_RESERVATION)
            Multihash ourId = Multihash.deserialize(node.getPeerId().getBytes());
            Connection relayed = null;
            long deadline = System.currentTimeMillis() + 120_000;
            Set<Multihash> tried = new HashSet<>();
            while (relayed == null && System.currentTimeMillis() < deadline) {
                byte[] key = new byte[32];
                new Random().nextBytes(key);
                for (PeerAddresses peer : dht.findClosestPeers(key, 20, node)) {
                    if (peer.peerId.equals(ourId) || ! tried.add(peer.peerId))
                        continue;
                    Optional<Multiaddr> relayedAddr = peer.addresses.stream()
                            .filter(a -> a.toString().contains("p2p-circuit"))
                            .findFirst();
                    if (relayedAddr.isEmpty())
                        continue;
                    PeerId target = PeerId.fromBase58(peer.peerId.toBase58());
                    Multiaddr dialAddr = relayedAddr.get().has(io.libp2p.core.multiformats.Protocol.P2P)
                            ? relayedAddr.get() : relayedAddr.get().withP2P(target);
                    try {
                        relayed = node.getNetwork().connect(target, dialAddr).get(20, TimeUnit.SECONDS);
                        System.out.println("Established relayed connection to " + target + " via " + relayed.remoteAddress());
                        break;
                    } catch (Exception e) {
                        System.out.println("Relayed dial to " + target + " failed: " + e.getCause());
                    }
                }
                if (relayed == null)
                    Thread.sleep(1000);
            }
            Assume.assumeTrue("could not establish a relayed connection to any relay-only peer", relayed != null);
            Assert.assertTrue("connection should be relayed", relayed.remoteAddress().toString().contains("p2p-circuit"));

            // the relayed peer should now initiate DCUtR to hole-punch a direct connection to us
            Connection direct;
            try {
                direct = upgraded.get(60, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                Assume.assumeNoException("DCUtR did not complete a direct connection in this run", e);
                return;
            }
            System.out.println("DCUtR upgraded to direct connection: " + direct.remoteAddress());
            Assert.assertFalse("upgraded connection must be direct", direct.remoteAddress().toString().contains("p2p-circuit"));

            // ping over the direct connection
            PingController pinger = direct.muxerSession().createStream(new Ping())
                    .getController().get(15, TimeUnit.SECONDS);
            long latency = pinger.ping().get(15, TimeUnit.SECONDS);
            System.out.println("Ping over DCUtR direct connection: " + latency + "ms");
        } finally {
            dht.stopBootstrapThread();
            node.stop();
        }
    }
}
