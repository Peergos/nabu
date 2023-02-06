package org.peergos.protocol.ipns;

import org.peergos.*;
import org.peergos.protocol.dht.pb.*;

import java.util.*;
import java.util.stream.*;

public class GetResult {
    public final Optional<IpnsMapping> record;
    public final List<PeerAddresses> closerPeers;

    public GetResult(Optional<IpnsMapping> record, List<PeerAddresses> closerPeers) {
        this.record = record;
        this.closerPeers = closerPeers;
    }

    public static GetResult fromProtobuf(Dht.Message msg) {
        List<PeerAddresses> closerPeers = msg.getCloserPeersList().stream()
                .map(PeerAddresses::fromProtobuf)
                .collect(Collectors.toList());
        Optional<IpnsMapping> record = msg.hasRecord() ?
                IPNS.validateIpnsEntry(msg) :
                Optional.empty();
        return new GetResult(record, closerPeers);
    }
}
