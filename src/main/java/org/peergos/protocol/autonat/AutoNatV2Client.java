package org.peergos.protocol.autonat;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;
import org.peergos.protocol.autonat.pb.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * Drives our reachability verdict using AutoNAT v2: on each outbound connection we ask the peer to dial
 * one of our candidate (predicted) addresses and prove it by echoing a nonce over a dial-back stream.
 * Once enough distinct peers report a verified dial, the verdict is written to {@link ReachabilityManager}.
 */
public class AutoNatV2Client {

    private static final int VOTES_REQUIRED = 3;
    private static final long RECHECK_INTERVAL_MINUTES = 30;

    private final Host us;
    private final ReachabilityManager reachability;
    private final AutonatV2.Binding autonat;
    private final NonceRegistry nonces;
    private final Supplier<List<Multiaddr>> candidateAddresses;
    private final int votesRequired;

    private final Map<Multihash, Boolean> votes = new ConcurrentHashMap<>();
    private final Map<Multihash, Multiaddr> confirmedAddresses = new ConcurrentHashMap<>();
    private final Set<Multihash> inFlight = ConcurrentHashMap.newKeySet();
    private volatile Thread recheckThread;

    public AutoNatV2Client(Host us,
                           ReachabilityManager reachability,
                           AutonatV2.Binding autonat,
                           NonceRegistry nonces,
                           Supplier<List<Multiaddr>> candidateAddresses) {
        this(us, reachability, autonat, nonces, candidateAddresses, VOTES_REQUIRED);
    }

    public AutoNatV2Client(Host us,
                           ReachabilityManager reachability,
                           AutonatV2.Binding autonat,
                           NonceRegistry nonces,
                           Supplier<List<Multiaddr>> candidateAddresses,
                           int votesRequired) {
        this.us = us;
        this.reachability = reachability;
        this.autonat = autonat;
        this.nonces = nonces;
        this.candidateAddresses = candidateAddresses;
        this.votesRequired = votesRequired;
    }

    public void onConnection(Connection conn) {
        if (! conn.isInitiator())
            return;
        if (reachability.getReachability() != Reachability.UNKNOWN)
            return;
        probe(conn);
    }

    private void probe(Connection conn) {
        List<Multiaddr> candidates = candidateAddresses.get();
        if (candidates.isEmpty())
            return;
        Multihash peer = Multihash.deserialize(conn.secureSession().getRemoteId().getBytes());
        if (votes.containsKey(peer) || ! inFlight.add(peer))
            return;
        long nonce = ThreadLocalRandom.current().nextLong();
        CompletableFuture<Boolean> dialBack = nonces.expect(nonce);
        conn.muxerSession().createStream(autonat).getController()
                .thenCompose(controller -> controller.requestDial(candidates, nonce))
                .whenComplete((resp, err) -> {
                    inFlight.remove(peer);
                    nonces.forget(nonce);
                    if (err != null || resp == null)
                        return; // peer doesn't speak autonat v2, or the request failed
                    if (resp.getStatus() != Autonatv2.DialResponse.ResponseStatus.OK)
                        return; // refused / rejected - says nothing about reachability
                    if (resp.getDialStatus() == Autonatv2.DialStatus.OK && dialBack.getNow(false)) {
                        int idx = resp.getAddrIdx();
                        if (idx >= 0 && idx < candidates.size())
                            confirmedAddresses.put(peer, candidates.get(idx));
                        recordVote(peer, true);
                    } else if (resp.getDialStatus() == Autonatv2.DialStatus.E_DIAL_ERROR) {
                        recordVote(peer, false);
                    }
                    // E_DIAL_BACK_ERROR / UNUSED are inconclusive
                });
    }

    private void recordVote(Multihash peer, boolean reachable) {
        votes.put(peer, reachable);
        AutoNatClient.evaluate(votes.values(), votesRequired).ifPresent(verdict -> {
            List<Multiaddr> publicAddrs = verdict == Reachability.PUBLIC
                    ? confirmedAddresses.values().stream().distinct().collect(Collectors.toList())
                    : List.of();
            reachability.setReachability(verdict, publicAddrs);
        });
    }

    public synchronized void start() {
        if (recheckThread != null)
            return;
        recheckThread = new Thread(() -> {
            while (! Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(TimeUnit.MINUTES.toMillis(RECHECK_INTERVAL_MINUTES));
                } catch (InterruptedException e) {
                    return;
                }
                recheck();
            }
        }, "autonat-v2-client");
        recheckThread.setDaemon(true);
        recheckThread.start();
    }

    public synchronized void stop() {
        if (recheckThread != null) {
            recheckThread.interrupt();
            recheckThread = null;
        }
    }

    public void recheck() {
        votes.clear();
        confirmedAddresses.clear();
        for (Connection conn : us.getNetwork().getConnections())
            if (conn.isInitiator())
                probe(conn);
    }
}
