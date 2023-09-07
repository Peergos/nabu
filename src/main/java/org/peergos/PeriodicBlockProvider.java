package org.peergos;

import io.ipfs.cid.*;
import io.libp2p.core.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
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
    private final Queue<Cid> newBlocksToPublish;

    public PeriodicBlockProvider(long reprovideIntervalMillis,
                                 Supplier<Stream<Cid>> getBlocks,
                                 Host us,
                                 Kademlia dht,
                                 Queue<Cid> newBlocksToPublish) {
        this.reprovideIntervalMillis = reprovideIntervalMillis;
        this.getBlocks = getBlocks;
        this.us = us;
        this.dht = dht;
        this.newBlocksToPublish = newBlocksToPublish;
    }

    private final AtomicBoolean running = new AtomicBoolean(false);
    public void start() {
        running.set(true);
        new Thread(this::run, "CidReprovider").start();
        new Thread(this::provideNewBlocks, "NewCidProvider").start();
    }

    public void stop() {
        running.set(false);
    }

    public void run() {
        while (running.get()) {
            try {
                publish(getBlocks.get());
                Thread.sleep(reprovideIntervalMillis);
            } catch (Throwable e) {
                LOG.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    public void provideNewBlocks() {
        while (running.get()) {
            try {
                Cid c = newBlocksToPublish.poll();
                if (c != null) {
                    publish(Stream.of(c));
                }
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
