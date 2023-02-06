package org.peergos.protocol.ipns;

import io.ipfs.multihash.*;

public class IpnsMapping {
    public final Multihash publisher;
    public final IpnsRecord value;

    public IpnsMapping(Multihash publisher, IpnsRecord value) {
        this.publisher = publisher;
        this.value = value;
    }
}
