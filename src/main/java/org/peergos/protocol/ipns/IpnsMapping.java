package org.peergos.protocol.ipns;

import com.google.protobuf.InvalidProtocolBufferException;
import io.ipfs.multihash.*;
import org.peergos.protocol.ipns.pb.Ipns;

import java.nio.ByteBuffer;

public class IpnsMapping {
    public final Multihash publisher;
    public final IpnsRecord value;

    public IpnsMapping(Multihash publisher, IpnsRecord value) {
        this.publisher = publisher;
        this.value = value;
    }

    public byte[] getData() {
        try {
            Ipns.IpnsEntry entry = Ipns.IpnsEntry.parseFrom(ByteBuffer.wrap(value.raw));
            return entry.getData().toByteArray();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] getSignature() {
        try {
        Ipns.IpnsEntry entry = Ipns.IpnsEntry.parseFrom(ByteBuffer.wrap(value.raw));
            return entry.getSignatureV2().toByteArray();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
