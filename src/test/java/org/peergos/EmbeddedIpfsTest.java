package org.peergos;

import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.config.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class EmbeddedIpfsTest {

    @Test
    public void test() throws Exception {
        EmbeddedIpfs node1 = build(Collections.emptyList());
        node1.start();
        EmbeddedIpfs node2 = build(node1.node.listenAddresses()
                .stream()
                .map(a -> new MultiAddress(a.toString()))
                .collect(Collectors.toList()));
        node2.start();

        Cid block = node2.blockstore.put("G'day mate!".getBytes(), Cid.Codec.Raw).join();
        PeerId peerId2 = node2.node.getPeerId();
        List<HashedBlock> retrieved = node1.getBlocks(List.of(new Want(block)), Set.of(peerId2), false);
        Assert.assertTrue(retrieved.size() == 1);

        node1.stop();
        node2.stop();
    }

    public static EmbeddedIpfs build(List<MultiAddress> bootstrap) {
        int swarm = TestPorts.getPort();
        List<MultiAddress> swarmAddresses = List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + swarm));
        BlockRequestAuthoriser blockRequestAuthoriser = (c, b, p, a) -> CompletableFuture.completedFuture(true);
        HostBuilder builder = new HostBuilder().generateIdentity();
        PrivKey privKey = builder.getPrivateKey();
        PeerId peerId = builder.getPeerId();
        IdentitySection id = new IdentitySection(privKey.bytes(), peerId);
        return EmbeddedIpfs.build(new RamRecordStore(), new RamBlockstore(), swarmAddresses, bootstrap, id, blockRequestAuthoriser, Optional.empty());
    }
}
