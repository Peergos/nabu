package org.peergos.protocol.circuit;

import io.ipfs.multihash.*;
import io.libp2p.core.*;

public interface StreamUpgrader {
    void upgrade(Stream s, Multihash targetPeerid, int durationSeconds, long limitBytes);
}
