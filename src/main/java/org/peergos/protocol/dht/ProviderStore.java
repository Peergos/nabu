package org.peergos.protocol.dht;

import io.ipfs.multihash.*;
import org.peergos.protocol.dht.pb.*;

import java.util.*;

public interface ProviderStore {

    void addProvider(Multihash m, Dht.Message.Peer peer);

    Set<Dht.Message.Peer> getProviders(Multihash m);
}
