package org.peergos.protocol.dht;

import io.ipfs.multihash.*;
import org.peergos.*;

import java.util.*;

public interface ProviderStore {

    void addProvider(Multihash m, PeerAddresses peer);

    Set<PeerAddresses> getProviders(Multihash m);
}
