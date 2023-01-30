package org.peergos.protocol.dht;

import io.ipfs.cid.*;
import org.peergos.protocol.ipns.*;

import java.util.*;

public interface RecordStore {

    void put(Cid peerId, IpnsRecord record);

    Optional<IpnsRecord> get(Cid peerId);
}
