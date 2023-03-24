package org.peergos.protocol.dht;

import com.offbynull.kademlia.Id;
import org.peergos.PeerAddresses;

record RoutingEntry(Id key, PeerAddresses addresses) {
}
