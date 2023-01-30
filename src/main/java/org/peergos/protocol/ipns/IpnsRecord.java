package org.peergos.protocol.ipns;

import org.jetbrains.annotations.*;

import java.time.*;

public class IpnsRecord implements Comparable<IpnsRecord> {

    public final byte[] raw;
    public final long sequence, ttlNanos;
    public final LocalDateTime expiry;
    public final String value;

    public IpnsRecord(byte[] raw, long sequence, long ttlNanos, LocalDateTime expiry, String value) {
        this.raw = raw;
        this.sequence = sequence;
        this.ttlNanos = ttlNanos;
        this.expiry = expiry;
        this.value = value;
    }

    @Override
    public int compareTo(@NotNull IpnsRecord b) {
        if (sequence != b.sequence)
            return (int)(sequence - b.sequence);
        if (expiry.isBefore(b.expiry))
            return -1;
        return 0;
    }
}
