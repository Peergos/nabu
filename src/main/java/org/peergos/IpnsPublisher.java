package org.peergos;

import io.ipfs.multiaddr.*;
import io.ipfs.multihash.*;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.crypto.keys.*;
import org.peergos.blockstore.*;
import org.peergos.config.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.ipns.*;
import org.peergos.util.*;

import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

public class IpnsPublisher {
    private static final ExecutorService ioExec = Executors.newFixedThreadPool(20);
    public static void main(String[] a) throws Exception {
        Path keysFile = Paths.get("publishers.txt");
        List<PrivKey> keys;
        int keycount = 1000;
        EmbeddedIpfs ipfs = startIpfs();
        if (keysFile.toFile().exists()) {
            List<String> lines = Files.readAllLines(keysFile);
            keys = lines.stream()
                    .map(line -> KeyKt.unmarshalPrivateKey(ArrayOps.hexToBytes(line)))
                    .collect(Collectors.toList());

            for (int c=0; c < 100; c++) {
                long t0 = System.currentTimeMillis();
                List<Integer> recordCounts = resolve(keys, ipfs);
                Files.write(Paths.get("publish-resolve-counts-" + LocalDateTime.now().withNano(0) + ".txt"), recordCounts.stream()
                        .map(i -> i.toString())
                        .collect(Collectors.toList()));
                long t1 = System.currentTimeMillis();
                System.out.println("Resolved " + recordCounts.stream().filter(n -> n > 0).count() + "/" + recordCounts.size()
                        + " in " + (t1-t0)/1000 + "s");
            }
        } else {
            keys = IntStream.range(0, keycount)
                    .mapToObj(i -> Ed25519Kt.generateEd25519KeyPair().getFirst())
                    .collect(Collectors.toList());
            Files.write(keysFile, keys.stream().map(k -> ArrayOps.bytesToHex(k.bytes())).collect(Collectors.toList()));
            long t0 = System.currentTimeMillis();
            List<Integer> publishCounts = publish(keys, "The result".getBytes(), ipfs);
            long t1 = System.currentTimeMillis();
            System.out.println("Published all in " + (t1-t0)/1000 + "s");
            Files.write(Paths.get("publish-counts.txt"), publishCounts.stream()
                    .map(i -> i.toString())
                    .collect(Collectors.toList()));
        }
        ipfs.stop().join();
        System.exit(0);
    }

    public static List<Integer> publish(List<PrivKey> publishers, byte[] value, EmbeddedIpfs ipfs) throws IOException {
        LocalDateTime expiry = LocalDateTime.now().plusDays(7);
        AtomicLong done = new AtomicLong(0);
        long ttlNanos = 7L * 24 * 3600 * 1000_000_000;
        List<Pair<Multihash, byte[]>> values = publishers.stream()
                .map(p -> new Pair<>(Multihash.deserialize(PeerId.fromPubKey(p.publicKey()).getBytes()),
                        IPNS.createSignedRecord(value, expiry, 1, ttlNanos, p)))
                .collect(Collectors.toList());
        Files.write(Paths.get("publish-values.txt"), values.stream()
                .map(v -> ArrayOps.bytesToHex(v.right))
                .collect(Collectors.toList()));
        List<CompletableFuture<Integer>> futs = values.stream()
                .map(v -> CompletableFuture.supplyAsync(() -> {
                    Integer res = ipfs.publishPresignedRecord(v.left, v.right).join();
                    System.out.println(done.incrementAndGet());
                    return res;
                }, ioExec))
                .collect(Collectors.toList());
        return futs.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
    }

    public static List<Integer> resolve(List<PrivKey> publishers, EmbeddedIpfs ipfs) {
        List<Integer> res = new ArrayList<>();
        for (PrivKey publisher : publishers) {
            List<IpnsRecord> records = ipfs.resolveRecords(publisher.publicKey(), 30);
            res.add(records.size());
        }
        return res;
    }

    public static EmbeddedIpfs startIpfs() {
        HostBuilder builder = new HostBuilder().generateIdentity();
        PrivKey privKey = builder.getPrivateKey();
        PeerId peerId = builder.getPeerId();
        IdentitySection id = new IdentitySection(privKey.bytes(), peerId);
        EmbeddedIpfs ipfs = EmbeddedIpfs.build(new RamRecordStore(), new RamBlockstore(), false,
                List.of(new MultiAddress("/ip6/::/tcp/0")), Config.defaultBootstrapNodes, id,
                (c, s, au) -> CompletableFuture.completedFuture(true), Optional.empty());
        ipfs.start();
        return ipfs;
    }
}
