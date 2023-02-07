package org.peergos.protocol.ports;

import com.offbynull.portmapper.*;
import com.offbynull.portmapper.gateway.*;
import com.offbynull.portmapper.gateways.network.*;
import com.offbynull.portmapper.gateways.process.*;
import com.offbynull.portmapper.mapper.*;

import java.util.*;

public class PortMapping {

    public static void startPortForwarder(int externalPort, int internalPort) throws InterruptedException {
        Gateway network = NetworkGateway.create();
        Gateway process = ProcessGateway.create();
        Bus networkBus = network.getBus();
        Bus processBus = process.getBus();

        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    List<PortMapper> mappers = PortMapperFactory.discover(networkBus, processBus);
                    Map<PortMapper, List<MappedPort>> mappings = new HashMap<>();
                    int lifetimeSeconds = 3600;
                    for (PortMapper mapper : mappers) {
                        mappings.putIfAbsent(mapper, new ArrayList<>());
                        mappings.get(mapper).add(mapper.mapPort(PortType.TCP, internalPort, externalPort, lifetimeSeconds));
                        mappings.get(mapper).add(mapper.mapPort(PortType.UDP, internalPort, externalPort, lifetimeSeconds));
                    }
                    // Refresh mapping half-way through the lifetime of the mapping
                    while (true) {
                        long minLifetime = mappings.values()
                                .stream()
                                .flatMap(List::stream)
                                .map(m -> m.getLifetime())
                                .mapToLong(i -> i).min().getAsLong();
                        for (PortMapper mapper : mappings.keySet()) {
                            List<MappedPort> mappedPorts = mappings.get(mapper);
                            for (MappedPort m : mappedPorts) {
                                mappedPorts.set(mappedPorts.indexOf(m), mapper.refreshPort(m, m.getLifetime()));
                            }
                        }
                        Thread.sleep(minLifetime / 2L * 1000L);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, "Port-mapper");
        thread.setDaemon(true);
        thread.start();
    }
}
