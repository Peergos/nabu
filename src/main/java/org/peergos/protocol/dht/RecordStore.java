package org.peergos.protocol.dht;

import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import org.peergos.protocol.ipns.*;

import java.util.*;

public interface RecordStore {

    void put(Multihash peerId, IpnsRecord record);

    Optional<IpnsRecord> get(Cid peerId);

    void remove(Multihash peerId);
}
