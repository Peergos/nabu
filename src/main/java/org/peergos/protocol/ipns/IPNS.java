package org.peergos.protocol.ipns;

import io.ipfs.multihash.*;
import org.peergos.cbor.*;

import java.io.*;
import java.nio.charset.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

public class IPNS {
    private static final DateTimeFormatter rfc3339nano = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.n");
    
    public static String formatExpiry(LocalDateTime expiry) {
        return expiry.atOffset(ZoneOffset.UTC).format(rfc3339nano)+"Z";
    }

    public static byte[] getKey(Multihash peerId) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            bout.write("/ipns/".getBytes());
            bout.write(peerId.toBytes());
        } catch (IOException e) {}
        return bout.toByteArray();
    }

    public static byte[] createCborDataForIpnsEntry(String pathToPublish,
                                                    LocalDateTime expiry,
                                                    long validityType,
                                                    long sequence,
                                                    long ttl) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));
        return CborObject.CborMap.build(state).serialize();
    }

    public static byte[] createSigV2Data(byte[] data) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            bout.write("ipns-signature:".getBytes(StandardCharsets.UTF_8));
            bout.write(data);
            return bout.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
