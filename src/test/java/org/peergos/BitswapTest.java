package org.peergos;

import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.dht.*;

import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class BitswapTest {
    private static final Random rnd = new Random(28);

    @Test
    public void getBlock() {
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true));
        Host node1 = builder1.build();
        RamBlockstore blockstore2 = new RamBlockstore();
        HostBuilder builder2 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), blockstore2, (c, p, a) -> CompletableFuture.completedFuture(true));
        Host node2 = builder2.build();
        node1.start().join();
        node2.start().join();
        try {
            Multiaddr address2 = node2.listenAddresses().get(0);
            byte[] blockData = "G'day from Java bitswap!".getBytes(StandardCharsets.UTF_8);
            Cid hash = blockstore2.put(blockData, Cid.Codec.Raw).join();
            Bitswap bitswap1 = builder1.getBitswap().get();
            node1.getAddressBook().addAddrs(address2.getPeerId(), 0, address2).join();
            List<HashedBlock> receivedBlock = bitswap1.get(List.of(new Want(hash)), node1, Set.of(address2.getPeerId()), false)
                    .stream()
                    .map(f -> f.join())
                    .collect(Collectors.toList());
            if (! Arrays.equals(receivedBlock.get(0).block, blockData))
                throw new IllegalStateException("Incorrect block returned!");
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void getTenBlocks() {
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true));
        Host node1 = builder1.build();
        RamBlockstore blockstore2 = new RamBlockstore();
        HostBuilder builder2 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), blockstore2, (c, p, a) -> CompletableFuture.completedFuture(true));
        Host node2 = builder2.build();
        node1.start().join();
        node2.start().join();
        try {
            Multiaddr address2 = node2.listenAddresses().get(0);
            List<Cid> hashes = new ArrayList<>();
            Random random = new Random(28);
            for (int i=0; i < 10; i++) {
                byte[] blockData = new byte[1024*1024];
                random.nextBytes(blockData);
                Cid hash = blockstore2.put(blockData, Cid.Codec.Raw).join();
                hashes.add(hash);
            }

            Bitswap bitswap1 = builder1.getBitswap().get();
            node1.getAddressBook().addAddrs(address2.getPeerId(), 0, address2).join();
            List<HashedBlock> receivedBlocks = bitswap1.get(hashes.stream().map(Want::new).collect(Collectors.toList()), node1, Set.of(address2.getPeerId()), false)
                    .stream()
                    .map(f -> f.join())
                    .collect(Collectors.toList());
            if (receivedBlocks.size() != hashes.size())
                throw new IllegalStateException("Incorrect number of blocks returned!");
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void blockFlooder() {
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true));
        Host flooder = builder1.build();
        RamBlockstore blockstore2 = new RamBlockstore();
        HostBuilder builder2 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), blockstore2, (c, p, a) -> CompletableFuture.completedFuture(true), true, Optional.empty());
        Host node2 = builder2.build();
        flooder.start().join();
        node2.start().join();
        try {
            Multiaddr address2 = node2.listenAddresses().get(0);
            byte[] blockData = "G'day from Java bitswap!".getBytes(StandardCharsets.UTF_8);
            Cid hash = blockstore2.put(blockData, Cid.Codec.Raw).join();
            Bitswap bitswap1 = builder1.getBitswap().get();
            flooder.getAddressBook().addAddrs(address2.getPeerId(), 0, address2).join();

            // flood with irrelevant requests
            List<CompletableFuture<HashedBlock>> fails = bitswap1.get(random(20), flooder, Set.of(address2.getPeerId()), false);

            // now try and get block
            try {
                List<HashedBlock> receivedBlockAgain = bitswap1.get(List.of(new Want(hash)), flooder, Set.of(address2.getPeerId()), false)
                        .stream()
                        .map(f -> f.orTimeout(1, TimeUnit.SECONDS).join())
                        .collect(Collectors.toList());
                if (Arrays.equals(receivedBlockAgain.get(0).block, blockData))
                    throw new IllegalStateException("Received block!");
            } catch (CompletionException t) {
                if (! (t.getCause() instanceof TimeoutException))
                    throw t;
            }
        } finally {
            flooder.stop();
            node2.stop();
        }
    }

    private List<Want> random(int n) {
        return IntStream.range(0, n)
                .mapToObj(i -> new Want(new Cid(1, Cid.Codec.Raw, Multihash.Type.sha2_256, randomBytes(32))))
                .collect(Collectors.toList());
    }

    private byte[] randomBytes(int n) {
        byte[] bytes = new byte[n];
        rnd.nextBytes(bytes);
        return bytes;
    }
}
