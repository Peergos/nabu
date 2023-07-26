package org.peergos.protocol;

import identify.pb.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import io.libp2p.etc.types.*;
import io.libp2p.protocol.*;

import java.util.stream.*;

public class IdentifyBuilder {

    public static void addIdentifyProtocol(Host node) {
        IdentifyOuterClass.Identify.Builder identifyBuilder = IdentifyOuterClass.Identify.newBuilder()
                .setProtocolVersion("ipfs/0.1.0")
                .setAgentVersion("nabu/v0.1.0")
                .setPublicKey(ByteArrayExtKt.toProtobuf(node.getPrivKey().publicKey().bytes()))
                .addAllListenAddrs(node.listenAddresses().stream()
                        .map(Multiaddr::serialize)
                        .map(ByteArrayExtKt::toProtobuf)
                        .collect(Collectors.toList()));

        for (ProtocolBinding<?> protocol : node.getProtocols()) {
            identifyBuilder = identifyBuilder.addAllProtocols(protocol.getProtocolDescriptor().getAnnounceProtocols());
        }
        Identify identify = new Identify(identifyBuilder.build());
        node.addProtocolHandler(identify);
    }
}
