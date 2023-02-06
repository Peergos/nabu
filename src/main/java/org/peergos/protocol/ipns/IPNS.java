package org.peergos.protocol.ipns;

import com.google.protobuf.*;
import crypto.pb.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import io.libp2p.core.crypto.*;
import io.libp2p.crypto.keys.*;
import org.peergos.cbor.*;
import org.peergos.protocol.dht.pb.*;
import org.peergos.protocol.ipns.pb.*;

import java.io.*;
import java.nio.charset.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

public class IPNS {
    public static final int MAX_RECORD_SIZE = 10*1024;

    public static final DateTimeFormatter rfc3339nano = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.n");
    
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

    public static Cid getCidFromKey(ByteString key) {
        if (! key.startsWith(ByteString.copyFrom("/ipns/".getBytes(StandardCharsets.UTF_8))))
            throw new IllegalStateException("Unknown IPNS key space: " + key);
        return Cid.cast(key.substring(6).toByteArray());
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

    public static Optional<IpnsMapping> validateIpnsEntry(Dht.Message msg) {
        if (! msg.hasRecord() || msg.getRecord().getValue().size() > IPNS.MAX_RECORD_SIZE)
            return Optional.empty();
        if (! msg.getKey().equals(msg.getRecord().getKey()))
            return Optional.empty();
        if (! msg.getRecord().getKey().startsWith(ByteString.copyFrom("/ipns/".getBytes(StandardCharsets.UTF_8))))
            return Optional.empty();
        byte[] cidBytes = msg.getRecord().getKey().substring(6).toByteArray();
        Multihash signer = Multihash.deserialize(cidBytes);
        try {
            Ipns.IpnsEntry entry = Ipns.IpnsEntry.parseFrom(msg.getRecord().getValue());
            if (! entry.hasSignatureV2() || ! entry.hasData())
                return Optional.empty();
            PubKey pub;
            if (signer.getType() == Multihash.Type.id) {
                byte[] pubKeymaterial = Arrays.copyOfRange(signer.getHash(), 4, 36);
                pub = new Ed25519PublicKey(new org.bouncycastle.crypto.params.Ed25519PublicKeyParameters(pubKeymaterial, 0));
            } else {
                Crypto.PublicKey publicKey = Crypto.PublicKey.parseFrom(entry.getPubKey());
                pub = RsaKt.unmarshalRsaPublicKey(publicKey.getData().toByteArray());
            }
            if (! pub.verify(ByteString.copyFrom("ipns-signature:".getBytes()).concat(entry.getData()).toByteArray(),
                    entry.getSignatureV2().toByteArray()))
                return Optional.empty();
            CborObject cbor = CborObject.fromByteArray(entry.getData().toByteArray());
            if (! (cbor instanceof CborObject.CborMap))
                return Optional.empty();
            CborObject.CborMap map = (CborObject.CborMap) cbor;
            if (map.getLong("Sequence") != entry.getSequence())
                return Optional.empty();
            if (map.getLong("TTL") != entry.getTtl())
                return Optional.empty();
            if (map.getLong("ValidityType") != entry.getValidityType().getNumber())
                return Optional.empty();
            if (! Arrays.equals(map.getByteArray("Value"), entry.getValue().toByteArray()))
                return Optional.empty();
            byte[] validity = entry.getValidity().toByteArray();
            if (! Arrays.equals(map.getByteArray("Validity"), validity))
                return Optional.empty();
            LocalDateTime expiry = LocalDateTime.parse(new String(validity).substring(0, validity.length - 1), IPNS.rfc3339nano);
            if (expiry.isBefore(LocalDateTime.now()))
                return Optional.empty();
            byte[] entryBytes = msg.getRecord().getValue().toByteArray();
            IpnsRecord record = new IpnsRecord(entryBytes, entry.getSequence(), entry.getTtl(), expiry, entry.getValue().toStringUtf8());
            return Optional.of(new IpnsMapping(signer, record));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }
}
