package org.peergos.config;


import io.ipfs.multiaddr.MultiAddress;
import org.peergos.util.JsonHelper;

import java.util.*;
import java.util.stream.Collectors;

public class AddressesSection implements Jsonable {
    private final List<MultiAddress> swarmAddresses;
    public final MultiAddress apiAddress;
    public final MultiAddress gatewayAddress;
    public final Optional<MultiAddress> proxyTargetAddress;
    public final Optional<String> allowTarget;

    public AddressesSection(List<MultiAddress> swarmAddresses, MultiAddress apiAddress, MultiAddress gatewayAddress,
                            Optional<MultiAddress> proxyTargetAddress, Optional<String> allowTarget) {
        this.swarmAddresses = swarmAddresses;
        this.apiAddress = apiAddress;
        this.gatewayAddress = gatewayAddress;
        this.proxyTargetAddress = proxyTargetAddress;
        this.allowTarget = allowTarget;
    }

    public List<MultiAddress> getSwarmAddresses() {
        return new ArrayList<MultiAddress>(swarmAddresses);
    }

    public Map<String, Object> toJson() {
        Map addressesMap = new LinkedHashMap<>();
        addressesMap.put("API", apiAddress.toString());
        addressesMap.put("Gateway", gatewayAddress.toString());
        if (proxyTargetAddress.isPresent()) {
            addressesMap.put("ProxyTarget", proxyTargetAddress.get().toString());
        }
        if (allowTarget.isPresent()) {
            addressesMap.put("AllowTarget", allowTarget.get());
        }
        List<String> swarm = swarmAddresses.stream().map(b -> b.toString()).collect(Collectors.toList());
        addressesMap.put("Swarm", swarm);
        Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put("Addresses", addressesMap);
        return configMap;
    }

    public static AddressesSection fromJson(Map<String, Object> json) {
        List<Object> swarm = JsonHelper.getPropertyList(json, "Addresses", "Swarm");
        List<MultiAddress> swarmAddresses = swarm.stream()
                .map(n -> new MultiAddress((String) n)).collect(Collectors.toList());
        MultiAddress api = new MultiAddress(JsonHelper.getStringProperty(json, "Addresses", "API"));
        MultiAddress gateway = new MultiAddress(JsonHelper.getStringProperty(json, "Addresses", "Gateway"));
        Optional<Object> proxyTarget = JsonHelper.getOptionalProperty(json, "Addresses", "ProxyTarget");
        Optional<Object> allowTarget = JsonHelper.getOptionalProperty(json, "Addresses", "AllowTarget");

        return new AddressesSection(swarmAddresses, api, gateway,
                proxyTarget.isPresent() ? Optional.of(new MultiAddress((String) proxyTarget.get())) : Optional.empty(),
                allowTarget.isPresent() ? Optional.of((String) allowTarget.get()) : Optional.empty()
        );
    }
}
