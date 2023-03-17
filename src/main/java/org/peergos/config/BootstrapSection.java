package org.peergos.config;


import io.ipfs.multiaddr.MultiAddress;
import org.peergos.util.JsonHelper;

import java.util.*;
import java.util.stream.Collectors;

public class BootstrapSection implements Jsonable {
    private final List<MultiAddress> bootstrapAddresses;

    public BootstrapSection(List<MultiAddress> bootstrapAddresses) {
        this.bootstrapAddresses = bootstrapAddresses;
    }

    public List<MultiAddress> getBootstrapAddresses() {
        return new ArrayList<MultiAddress>(bootstrapAddresses);
    }

    public Map<String, Object> toJson() {
        Map<String, Object> configMap = new LinkedHashMap<>();
        List<String> nodes = bootstrapAddresses.stream().map(b -> b.toString()).collect(Collectors.toList());
        configMap.put("Bootstrap", nodes);
        return configMap;
    }

    public static BootstrapSection fromJson(Map<String, Object> json) {
        List<Object> nodes = JsonHelper.getPropertyList(json, "Bootstrap");
        List<MultiAddress> multiAddressNodes = nodes.stream()
                .map(n -> new MultiAddress((String) n)).collect(Collectors.toList());
        return new BootstrapSection(multiAddressNodes);
    }
}
