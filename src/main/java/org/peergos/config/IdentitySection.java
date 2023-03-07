package org.peergos.config;


import io.libp2p.core.PeerId;
import org.peergos.util.JsonHelper;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

public class IdentitySection implements Jsonable {
    public final byte[] privKey;
    public final PeerId peerId;

    public IdentitySection(byte[] privKey, PeerId peerId) {
        this.privKey = privKey;
        this.peerId = peerId;
    }

    public Map<String, Object> toJson() {
        Map<String, Object> identityMap = new LinkedHashMap<>();
        identityMap.put("PeerID", peerId.toBase58());
        String base64PrivKeyStr = Base64.getEncoder().encodeToString(privKey);
        identityMap.put("PrivKey", base64PrivKeyStr);
        Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put("Identity", identityMap);
        return configMap;
    }

    public static IdentitySection fromJson(Map<String, Object> json) {
        String base64PrivKey = JsonHelper.getStringProperty(json, "Identity", "PrivKey");
        byte[] privKey = io.ipfs.multibase.binary.Base64.decodeBase64(base64PrivKey);
        String base58PeerID = JsonHelper.getStringProperty(json, "Identity", "PeerID");
        return new IdentitySection(privKey, PeerId.fromBase58(base58PeerID));
    }
}
