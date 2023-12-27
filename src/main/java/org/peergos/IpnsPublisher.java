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
import java.util.stream.*;
import java.util.stream.Stream;

public class IpnsPublisher {
    private static final ExecutorService ioExec = Executors.newFixedThreadPool(10);
    public static void main(String[] a) throws Exception {
        Path publishFile = Paths.get("publishers.txt");
        int keycount = 1000;
        EmbeddedIpfs publisher = startIpfs();
        EmbeddedIpfs resolver = startIpfs();
        if (publishFile.toFile().exists()) {
            List<String> lines = Files.readAllLines(publishFile);
            List<PublishResult> records = lines.stream()
                    .map(line -> new PublishResult(KeyKt.unmarshalPrivateKey(ArrayOps.hexToBytes(line.split(" ")[0])),
                            Multihash.fromBase58(line.split(" ")[1]),
                            ArrayOps.hexToBytes(line.split(" ")[2]),
                            0))
                    .collect(Collectors.toList());

            System.out.println("Resolving " + records.size() + " keys");
            for (int c=0; c < 100; c++) {
                long t0 = System.currentTimeMillis();
                List<Integer> recordCounts = resolveAndRepublish(records, resolver, publisher);
                Path resultsFile = Paths.get("publish-resolve-counts-" + LocalDateTime.now().withNano(0) + ".txt");
                Files.write(resultsFile,
                        recordCounts.stream()
                                .map(Object::toString)
                                .collect(Collectors.toList()));
                long t1 = System.currentTimeMillis();
                String total = "Resolved " + recordCounts.stream().filter(n -> n > 0).count() + "/" + recordCounts.size()
                        + " in " + (t1 - t0) / 1000 + "s";
                System.out.println(total);
                Files.write(resultsFile, total.getBytes(), StandardOpenOption.APPEND);
                String fails = "\nFailed " + IntStream.range(0, recordCounts.size())
                        .filter(i -> recordCounts.get(i) == 0)
                        .mapToObj(i -> i)
                        .collect(Collectors.toList());
                System.out.println(fails);
                Files.write(resultsFile, fails.getBytes(), StandardOpenOption.APPEND);
            }
        } else {
            List<PrivKey> keys = IntStream.range(0, keycount)
                    .mapToObj(i -> Ed25519Kt.generateEd25519KeyPair().getFirst())
                    .collect(Collectors.toList());
            long t0 = System.currentTimeMillis();
            List<CompletableFuture<PublishResult>> futs = publish(keys, "The result".getBytes(), publisher)
                    .collect(Collectors.toList());
            futs.forEach(res -> {
                try {
                    Files.write(publishFile, res.join().toString().getBytes(),
                            publishFile.toFile().exists() ?
                                    StandardOpenOption.APPEND :
                                    StandardOpenOption.CREATE_NEW);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            long t1 = System.currentTimeMillis();
            System.out.println("Published all in " + (t1-t0)/1000 + "s");
        }
        publisher.stop().join();
        System.exit(0);
    }

    public static class PublishResult {
        public final PrivKey priv;
        public final Multihash pub;
        public final byte[] record;
        public final int publishCount;

        public PublishResult(PrivKey priv, Multihash pub, byte[] record, int publishCount) {
            this.priv = priv;
            this.pub = pub;
            this.record = record;
            this.publishCount = publishCount;
        }

        @Override
        public String toString() {
            return ArrayOps.bytesToHex(priv.bytes()) + " " + pub + " " + ArrayOps.bytesToHex(record)
                    + " " + publishCount + "\n";
        }
    }

    public static Stream<CompletableFuture<PublishResult>> publish(List<PrivKey> publishers, byte[] value, EmbeddedIpfs ipfs) throws IOException {
        LocalDateTime expiry = LocalDateTime.now().plusDays(7);
        long ttlNanos = 7L * 24 * 3600 * 1000_000_000;
        List<PublishResult> signed = publishers.stream()
                .map(p -> {
                    Multihash pub = Multihash.deserialize(PeerId.fromPubKey(p.publicKey()).getBytes());
                    byte[] record = IPNS.createSignedRecord(value, expiry, 1, ttlNanos, p);
                    return new PublishResult(p, pub, record, 0);
                }).collect(Collectors.toList());
        return publish(signed, ipfs);
    }

    public static Stream<CompletableFuture<PublishResult>> publish(List<PublishResult> pubs, EmbeddedIpfs ipfs) {
        return pubs.stream()
                .map(p -> publish(p, ipfs));
    }

    public static CompletableFuture<PublishResult> publish(PublishResult signed, EmbeddedIpfs ipfs) {
        return CompletableFuture.supplyAsync(() -> {
            int count = ipfs.publishPresignedRecord(signed.pub, signed.record).join();
            return new PublishResult(signed.priv, signed.pub, signed.record, count);
        }, ioExec);
    }

    public static List<Integer> resolveAndRepublish(List<PublishResult> publishers,
                                                    EmbeddedIpfs resolver,
                                                    EmbeddedIpfs publisher) {
        List<Integer> res = new ArrayList<>();
        int done = 0;
        for (PublishResult pub : publishers) {
            List<IpnsRecord> records = resolver.resolveRecords(pub.priv.publicKey(), 30);
            res.add(records.size());
            done++;
            if (done % 10 == 0)
                System.out.println("resolved " + done);
            CompletableFuture.supplyAsync(() -> publisher.publishPresignedRecord(pub.pub, pub.record));
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
