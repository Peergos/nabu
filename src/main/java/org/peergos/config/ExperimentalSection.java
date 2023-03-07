package org.peergos.config;

import io.libp2p.core.PeerId;
import org.peergos.util.JsonHelper;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class ExperimentalSection implements Jsonable {
    public final boolean libp2pStreamMounting;
    public final boolean p2pHttpProxy;

    public ExperimentalSection(boolean libp2pStreamMounting, boolean p2pHttpProxy) {
        this.libp2pStreamMounting = libp2pStreamMounting;
        this.p2pHttpProxy = p2pHttpProxy;
    }

    public Map<String, Object> toJson() {
        Map<String, Object> experimentalMap = new LinkedHashMap<>();
        experimentalMap.put("Libp2pStreamMounting", libp2pStreamMounting);
        experimentalMap.put("P2pHttpProxy", p2pHttpProxy);
        Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put("Experimental", experimentalMap);
        return configMap;
    }

    public static ExperimentalSection fromJson(Map<String, Object> json) {
        Optional<Object> libp2pStreamMounting = JsonHelper.getOptionalProperty(json, "Experimental", "Libp2pStreamMounting");
        Optional<Object> p2pHttpProxy = JsonHelper.getOptionalProperty(json, "Experimental", "P2pHttpProxy");
        return new ExperimentalSection(
                libp2pStreamMounting.isPresent() ? (Boolean) libp2pStreamMounting.get() : false,
                p2pHttpProxy.isPresent() ? (Boolean) p2pHttpProxy.get() : false
        );
    }
}
