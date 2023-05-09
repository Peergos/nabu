package org.peergos;

import io.ipfs.cid.*;
import io.libp2p.core.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.logging.*;
import java.util.stream.*;
import java.util.stream.Stream;

public class PeriodicBlockProvider {

    private static final Logger LOG = Logger.getLogger(PeriodicBlockProvider.class.getName());
    private final long reprovideIntervalMillis;
    private final Supplier<Stream<Cid>> getBlocks;
    private final Host us;
    private final Kademlia dht;

    public PeriodicBlockProvider(long reprovideIntervalMillis, Supplier<Stream<Cid>> getBlocks, Host us, Kademlia dht) {
        this.reprovideIntervalMillis = reprovideIntervalMillis;
        this.getBlocks = getBlocks;
        this.us = us;
        this.dht = dht;
    }

    public void start() {
        new Thread(this::run, "CidPublisher").start();
    }

    public void run() {
        while (true) {
            try {
                publish(getBlocks.get());
                Thread.sleep(reprovideIntervalMillis);
            } catch (Throwable e) {
                LOG.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    public void publish(Stream<Cid> blocks) {
        PeerAddresses ourAddrs = PeerAddresses.fromHost(us);

        List<CompletableFuture<Void>> published = blocks.parallel()
                .map(ref -> dht.provideBlock(ref, us, ourAddrs))
                .collect(Collectors.toList());
        for (CompletableFuture<Void> fut : published) {
            fut.join();
        }
    }
}
