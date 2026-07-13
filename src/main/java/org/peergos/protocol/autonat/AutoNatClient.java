package org.peergos.protocol.autonat;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.peergos.*;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * Drives our own reachability verdict by asking AutoNAT servers to dial us back.
 *
 * On each outbound connection we opportunistically ask the remote peer (a candidate AutoNAT server) to
 * dial back our claimed public addresses. Once enough distinct peers agree, the verdict is written to
 * the {@link ReachabilityManager}. A background thread periodically clears the votes and re-probes, so
 * the verdict adapts as the network changes.
 */
public class AutoNatClient {

    private static final int VOTES_REQUIRED = 3;
    private static final long RECHECK_INTERVAL_MINUTES = 30;

    private final Host us;
    private final Multihash ourPeerId;
    private final ReachabilityManager reachability;
    private final AutonatProtocol.Binding autonat;
    private final Supplier<List<Multiaddr>> candidateAddresses;
    private final int votesRequired;

    // One vote per distinct peer: true == they reached us, false == dial-back failed.
    private final Map<Multihash, Boolean> votes = new ConcurrentHashMap<>();
    private final Map<Multihash, Multiaddr> confirmedAddresses = new ConcurrentHashMap<>();
    private final Set<Multihash> inFlight = ConcurrentHashMap.newKeySet();
    private volatile Thread recheckThread;

    public AutoNatClient(Host us,
                         Multihash ourPeerId,
                         ReachabilityManager reachability,
                         AutonatProtocol.Binding autonat,
                         Supplier<List<Multiaddr>> candidateAddresses) {
        this(us, ourPeerId, reachability, autonat, candidateAddresses, VOTES_REQUIRED);
    }

    public AutoNatClient(Host us,
                         Multihash ourPeerId,
                         ReachabilityManager reachability,
                         AutonatProtocol.Binding autonat,
                         Supplier<List<Multiaddr>> candidateAddresses,
                         int votesRequired) {
        this.us = us;
        this.ourPeerId = ourPeerId;
        this.reachability = reachability;
        this.autonat = autonat;
        this.candidateAddresses = candidateAddresses;
        this.votesRequired = votesRequired;
    }

    /** Aggregate collected votes into a verdict, if enough distinct peers agree. */
    public static Optional<Reachability> evaluate(Collection<Boolean> votes, int required) {
        long reachable = votes.stream().filter(v -> v).count();
        long unreachable = votes.stream().filter(v -> !v).count();
        if (reachable >= required)
            return Optional.of(Reachability.PUBLIC);
        if (unreachable >= required)
            return Optional.of(Reachability.PRIVATE);
        return Optional.empty();
    }

    /** Register on a host so every outbound connection is a probe opportunity. */
    public void onConnection(Connection conn) {
        if (! conn.isInitiator())
            return; // only peers we dialled are candidate AutoNAT servers
        if (reachability.getReachability() != Reachability.UNKNOWN)
            return; // already decided; the recheck thread will re-open probing
        probe(conn);
    }

    private void probe(Connection conn) {
        List<Multiaddr> candidates = candidateAddresses.get();
        if (candidates.isEmpty())
            return; // nothing to test yet (no observed public address)
        Multihash peer = Multihash.deserialize(conn.secureSession().getRemoteId().getBytes());
        if (votes.containsKey(peer) || ! inFlight.add(peer))
            return;
        PeerAddresses claim = new PeerAddresses(ourPeerId, candidates);
        conn.muxerSession().createStream(autonat).getController()
                .thenCompose(controller -> controller.requestDial(claim))
                .whenComplete((resp, err) -> {
                    inFlight.remove(peer);
                    if (err != null || resp == null)
                        return; // peer doesn't speak autonat, or the stream failed
                    switch (resp.getStatus()) {
                        case OK:
                            try {
                                confirmedAddresses.put(peer, Multiaddr.deserialize(resp.getAddr().toByteArray()));
                            } catch (Exception e) {}
                            recordVote(peer, true);
                            break;
                        case E_DIAL_ERROR:
                            recordVote(peer, false);
                            break;
                        default:
                            // E_DIAL_REFUSED / E_BAD_REQUEST / E_INTERNAL_ERROR say nothing about reachability
                    }
                });
    }

    private void recordVote(Multihash peer, boolean reachable) {
        votes.put(peer, reachable);
        evaluate(votes.values(), votesRequired).ifPresent(verdict -> {
            List<Multiaddr> publicAddrs = verdict == Reachability.PUBLIC
                    ? confirmedAddresses.values().stream().distinct().collect(Collectors.toList())
                    : List.of();
            reachability.setReachability(verdict, publicAddrs);
        });
    }

    /** Start the background thread that periodically re-probes so the verdict tracks network changes. */
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
        }, "autonat-client");
        recheckThread.setDaemon(true);
        recheckThread.start();
    }

    public synchronized void stop() {
        if (recheckThread != null) {
            recheckThread.interrupt();
            recheckThread = null;
        }
    }

    /** Clear accumulated votes and re-probe current outbound connections. */
    public void recheck() {
        votes.clear();
        confirmedAddresses.clear();
        for (Connection conn : us.getNetwork().getConnections()) {
            if (conn.isInitiator())
                probe(conn);
        }
    }
}
