package org.peergos.protocol.circuit;

import io.libp2p.core.PeerId;

import java.util.*;
import java.util.function.*;

/** What a relay forwards, metered by a token bucket per reserved target, counting all its circuits in both
 *  directions, and one for the relay as a whole. A limit on each circuit alone is evaded by opening another, and one
 *  per dialler by dialling a target of one's own from many peers, so it is the target's budget that is spent.
 */
public class RelayBandwidth {
    private static final int MAX_TARGETS = 10_000;

    public final long perTargetBytesPerSecond, perTargetBurst, totalBytesPerSecond, totalBurst;
    private final LongSupplier nanos;
    private final Map<PeerId, Bucket> byTarget = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<PeerId, Bucket> eldest) {
            return size() > MAX_TARGETS;
        }
    };
    private final Bucket total;

    public RelayBandwidth(long perTargetBytesPerSecond, long perTargetBurst, long totalBytesPerSecond, long totalBurst) {
        this(perTargetBytesPerSecond, perTargetBurst, totalBytesPerSecond, totalBurst, System::nanoTime);
    }

    public RelayBandwidth(long perTargetBytesPerSecond, long perTargetBurst, long totalBytesPerSecond, long totalBurst,
                          LongSupplier nanos) {
        this.perTargetBytesPerSecond = perTargetBytesPerSecond;
        this.perTargetBurst = perTargetBurst;
        this.totalBytesPerSecond = totalBytesPerSecond;
        this.totalBurst = totalBurst;
        this.nanos = nanos;
        this.total = new Bucket(totalBurst, nanos.getAsLong());
    }

    /** Charges bytes forwarded for a target, and returns the milliseconds until both buckets are out of debt. */
    public synchronized long charge(PeerId target, long bytes) {
        long now = nanos.getAsLong();
        Bucket b = byTarget.computeIfAbsent(target, t -> new Bucket(perTargetBurst, now));
        return Math.max(b.take(bytes, perTargetBytesPerSecond, perTargetBurst, now),
                total.take(bytes, totalBytesPerSecond, totalBurst, now));
    }

    private static final class Bucket {
        double tokens;
        long at;

        Bucket(long burst, long now) {
            this.tokens = burst;
            this.at = now;
        }

        /** Spending past empty is a debt, paid off at the rate before anything more is forwarded. */
        long take(long bytes, long perSecond, long burst, long now) {
            tokens = Math.min(burst, tokens + (now - at) * perSecond / 1e9);
            at = now;
            tokens -= bytes;
            return tokens >= 0 ? 0 : (long) Math.ceil(-tokens * 1000 / perSecond);
        }
    }
}
