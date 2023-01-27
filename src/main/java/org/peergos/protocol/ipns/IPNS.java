package org.peergos.protocol.ipns;

import org.peergos.cbor.*;

import java.io.*;
import java.nio.charset.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

public class IPNS {
    public static final DateTimeFormatter rfc3339nano = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nZ");

    public static byte[] createCborDataForIpnsEntry(String pathToPublish,
                                                    LocalDateTime expiry,
                                                    long validityType,
                                                    long sequence,
                                                    long ttl) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Validity", new CborObject.CborByteArray(expiry.atOffset(ZoneOffset.UTC).format(rfc3339nano).getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));
        state.put("Sequence", new CborObject.CborLong(sequence));
        state.put("TTL", new CborObject.CborLong(ttl));
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
