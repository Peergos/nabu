package org.peergos;

import io.ipfs.api.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.cbor.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.dht.*;

import java.io.*;
import java.util.*;
import java.util.stream.*;

public class BitswapMirrorTest {

    @Test
    //@Ignore // Local testing only for now - run this prior: ./ipfs pin add zdpuAwfJrGYtiGFDcSV3rDpaUrqCtQZRxMjdC6Eq9PNqLqTGg
    public void mirrorTree() throws IOException {
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), new RamBlockstore());
        Host node1 = builder1.build();
        node1.start().join();
        IPFS kubo = new IPFS("localhost", 5001);
        Multiaddr kuboAddress = Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001/p2p/" + kubo.id().get("ID"));
        try {
            Set<Cid> toGet = new HashSet<>();
            Set<Cid> rawToGet = new HashSet<>();
            toGet.add(Cid.decode("zdpuAwfJrGYtiGFDcSV3rDpaUrqCtQZRxMjdC6Eq9PNqLqTGg"));
            long t1 = System.currentTimeMillis();
            while (true) {
                Bitswap bitswap1 = builder1.getBitswap().get();
                BitswapController bc1 = bitswap1.dial(node1, kuboAddress).getController().join();
                List<CborObject> cborBlocks = bitswap1
                        .get(new ArrayList<>(toGet), node1).stream()
                        .map(f -> f.join())
                        .map(CborObject::fromByteArray)
                        .collect(Collectors.toList());
                List<byte[]> rawBlocks = bitswap1
                        .get(new ArrayList<>(rawToGet), node1).stream()
                        .map(f -> f.join())
                        .collect(Collectors.toList());
                toGet.clear();
                rawToGet.clear();
                cborBlocks.stream()
                        .flatMap(b -> b.links().stream())
                        .map(h -> (Cid)h)
                        .map(c -> c.codec == Cid.Codec.Raw ? rawToGet.add(c) : c.getType() == Multihash.Type.id || toGet.add(c))
                        .collect(Collectors.toSet());
                System.out.println("links cbor: " + toGet.size() + ", raw: " + rawToGet.size());
                if (toGet.isEmpty() && rawToGet.isEmpty())
                    break;
            }
            long t2 = System.currentTimeMillis();
            System.out.println("Mirror took " + (t2-t1) + "mS");
        } finally {
            node1.stop();
        }
    }
}
