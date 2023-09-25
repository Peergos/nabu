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
    private final Host us;
    private final Kademlia dht;
    private final Queue<Cid> newBlocksToPublish;

    private final ControlSubThread cidProvider;

    private final ControlSubThread newCidProvider;

    public PeriodicBlockProvider(long reprovideIntervalMillis,
                                 Supplier<Stream<Cid>> getBlocks,
                                 Host us,
                                 Kademlia dht,
                                 Queue<Cid> newBlocksToPublish) {
        this.us = us;
        this.dht = dht;
        this.newBlocksToPublish = newBlocksToPublish;
        this.cidProvider = new ControlSubThread(reprovideIntervalMillis, () -> { publish(getBlocks.get()); return null;}, "CidReprovider");
        this.newCidProvider = new ControlSubThread(0, () -> { provideNewBlocks(); return null;}, "NewCidProvider");
    }

    private final AtomicBoolean running = new AtomicBoolean(false);
    public void start() {
        running.set(true);
        cidProvider.start();
        newCidProvider.start();
    }

    public boolean isRunning() {
        return running.get();
    }

    public void stop() {
        running.set(false);
        if (cidProvider.isRunning()) {
            cidProvider.stop();
        }
        if (newCidProvider.isRunning()) {
            newCidProvider.stop();
        }
        while (!cidProvider.isStopped() || !newCidProvider.isStopped()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
        }
    }

    public void provideNewBlocks() {
        while (running.get()) {
            Cid c = newBlocksToPublish.poll();
            if (c != null) {
                publish(Stream.of(c));
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
