package org.peergos.protocol.dht;

import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import org.peergos.protocol.ipns.*;

import java.util.*;
import java.util.concurrent.*;

public class RamRecordStore implements RecordStore {

    private final Map<Multihash, IpnsRecord> records = new ConcurrentHashMap<>();

    @Override
    public void put(Multihash peerId, IpnsRecord record) {
        IpnsRecord existing = records.get(peerId);
        if (existing == null || existing.compareTo(record) < 0)
            records.put(peerId, record);
    }

    @Override
    public Optional<IpnsRecord> get(Cid peerId) {
        return Optional.ofNullable(records.get(peerId));
    }
}
