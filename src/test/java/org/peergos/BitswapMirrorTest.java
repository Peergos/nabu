package org.peergos;

import io.ipfs.api.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.cbor.*;
import org.peergos.protocol.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.dht.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class BitswapMirrorTest {

    @Test
    //@Ignore // Local testing only for now - run this prior: ./ipfs pin add zdpuAwfJrGYtiGFDcSV3rDpaUrqCtQZRxMjdC6Eq9PNqLqTGg
    public void mirrorTree() throws IOException {
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true));
        Host node1 = builder1.build();
        node1.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1, Collections.emptyList());
        IPFS kubo = new IPFS("localhost", 5001);
        Multiaddr kuboAddress = Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001/p2p/" + kubo.id().get("ID"));
        preloadBlocksToKubo(kubo);
//        Multiaddr kuboAddress = Multiaddr.fromString("/ip4/172.104.157.121/tcp/4001/p2p/QmVdFZgHnEgcedCS2G2ZNiEN59LuVrnRm7z3yXtEBv2XiF");
        node1.getAddressBook().setAddrs(kuboAddress.getPeerId(), 0, kuboAddress).join();
        Kademlia dht1 = builder1.getWanDht().get();
        dht1.bootstrapRoutingTable(node1, BootstrapTest.BOOTSTRAP_NODES, addr -> !addr.contains("/wss/"));
            dht1.bootstrap(node1);
        List<PeerAddresses> closestPeers = dht1.findClosestPeers(Multihash.deserialize(kuboAddress.getPeerId().getBytes()), 2, node1);
        Optional<PeerAddresses> matching = closestPeers.stream()
                .filter(p -> p.peerId.equals(kuboAddress.getPeerId()))
                .findFirst();
        try {
            Set<Want> toGet = new HashSet<>();
            Set<Want> rawToGet = new HashSet<>();
            toGet.add(new Want(Cid.decode("zdpuAwfJrGYtiGFDcSV3rDpaUrqCtQZRxMjdC6Eq9PNqLqTGg")));
            long t1 = System.currentTimeMillis();
            Map<Cid, byte[]> blocks = new HashMap<>();
            while (true) {
                Bitswap bitswap1 = builder1.getBitswap().get();
//                BitswapController bc1 = bitswap1.dial(node1, kuboAddress).getController().join();
                List<CborObject> cborBlocks = bitswap1
                        .get(new ArrayList<>(toGet), node1, Set.of(kuboAddress.getPeerId()), false).stream()
                        .map(f -> f.join())
                        .map(h -> {
                            blocks.put(h.hash, h.block);
                            return h.block;
                        })
                        .map(CborObject::fromByteArray)
                        .collect(Collectors.toList());
                List<byte[]> rawBlocks = bitswap1
                        .get(new ArrayList<>(rawToGet), node1, Set.of(kuboAddress.getPeerId()), false).stream()
                        .map(f -> f.join())
                        .map(h -> {
                            blocks.put(h.hash, h.block);
                            return h.block;
                        })
                        .collect(Collectors.toList());
                toGet.clear();
                rawToGet.clear();
                cborBlocks.stream()
                        .flatMap(b -> b.links().stream())
                        .map(h -> (Cid)h)
                        .map(c -> c.codec == Cid.Codec.Raw ?
                                rawToGet.add(new Want(c)) :
                                c.getType() == Multihash.Type.id || toGet.add(new Want(c)))
                        .collect(Collectors.toSet());
                System.out.println("links cbor: " + toGet.size() + ", raw: " + rawToGet.size());
                if (toGet.isEmpty() && rawToGet.isEmpty())
                    break;
            }
            long t2 = System.currentTimeMillis();
            System.out.println("Mirror took " + (t2-t1) + "mS");
            Assert.assertTrue(blocks.size() == 6745);
        } finally {
            node1.stop();
        }
    }

    private static void preloadBlocksToKubo(IPFS kubo) throws IOException {
        DataInputStream din = new DataInputStream(new FileInputStream("blocks.bin"));
        Map<Cid, byte[]> blocks = new HashMap<>();
        while (true) {
            try {
                int cidSize = din.readInt();
                byte[] rawCid = din.readNBytes(cidSize);
                int len = din.readInt();
                byte[] b = din.readNBytes(len);
                blocks.put(Cid.cast(rawCid), b);
            } catch (IOException e) {
                break;
            }
        }
        for (Map.Entry<Cid, byte[]> e : blocks.entrySet()) {
            kubo.block.put(e.getValue(), Optional.of(e.getKey().codec == Cid.Codec.Raw ? "raw" : "dag-cbor"));
        }
    }
}
