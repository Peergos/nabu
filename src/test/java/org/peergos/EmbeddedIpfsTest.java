package org.peergos;

import identify.pb.*;
import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.config.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class EmbeddedIpfsTest {

    @Test
    public void largeBlock() throws Exception {
        EmbeddedIpfs node1 = build(Collections.emptyList(), List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + TestPorts.getPort())));
        node1.start();
        EmbeddedIpfs node2 = build(node1.node.listenAddresses()
                .stream()
                .map(a -> new MultiAddress(a.toString()))
                .collect(Collectors.toList()), List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + TestPorts.getPort())));
        node2.start();

        Cid block = node2.blockstore.put(new byte[1024 * 1024], Cid.Codec.Raw).join();
        PeerId peerId2 = node2.node.getPeerId();
        List<HashedBlock> retrieved = ForkJoinPool.commonPool().submit(
                () -> node1.getBlocks(List.of(new Want(block)), Set.of(peerId2), false))
                .get(5, TimeUnit.SECONDS);
        Assert.assertTrue(retrieved.size() == 1);

        node1.stop();
        node2.stop();
    }

    @Test
    public void wildcardListenerAddressesGetExpanded() {
        int node1Port = TestPorts.getPort();
        EmbeddedIpfs node1 = build(Collections.emptyList(), List.of(new MultiAddress("/ip6/::/tcp/" + node1Port)));
        node1.start();

        EmbeddedIpfs node2 = build(node1.node.listenAddresses()
                .stream()
                .map(a -> new MultiAddress(a.toString()))
                .collect(Collectors.toList()), List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + TestPorts.getPort())));
        node2.start();
        Multiaddr node1Addr = new Multiaddr("/ip4/127.0.0.1/tcp/" + node1Port + "/p2p/" + node1.node.getPeerId());
        IdentifyOuterClass.Identify id = new Identify().dial(node2.node, node1Addr).getController().join().id().join();
        List<MultiAddress> listening = id.getListenAddrsList().stream().map(b -> new MultiAddress(b.toByteArray())).collect(Collectors.toList());
        Assert.assertTrue(listening.stream().anyMatch(a -> a.toString().contains("/ip4/127.0.0.1")));
        Assert.assertTrue(listening.stream().noneMatch(a -> a.toString().contains("/p2p/")));
        Assert.assertTrue(listening.stream().noneMatch(a -> a.toString().contains("/ipfs/")));
    }

    public static EmbeddedIpfs build(List<MultiAddress> bootstrap, List<MultiAddress> swarmAddresses) {
        BlockRequestAuthoriser blockRequestAuthoriser = (c, b, p, a) -> CompletableFuture.completedFuture(true);
        HostBuilder builder = new HostBuilder().generateIdentity();
        PrivKey privKey = builder.getPrivateKey();
        PeerId peerId = builder.getPeerId();
        IdentitySection id = new IdentitySection(privKey.bytes(), peerId);
        return EmbeddedIpfs.build(new RamRecordStore(), new RamBlockstore(), new RamBlockMetadataStore(), swarmAddresses, bootstrap, id, blockRequestAuthoriser, Optional.empty());
    }
}
