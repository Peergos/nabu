package org.peergos.protocol.ports;

import com.offbynull.portmapper.*;
import com.offbynull.portmapper.gateway.*;
import com.offbynull.portmapper.gateways.network.*;
import com.offbynull.portmapper.gateways.process.*;
import com.offbynull.portmapper.mapper.*;
import io.libp2p.core.multiformats.*;
import org.peergos.util.Logging;

import java.net.*;
import java.util.*;
import java.util.function.*;
import java.util.logging.*;

/**
 * Maps a swarm port through a UPnP/NAT-PMP gateway so a NATed node becomes dialable, and reports the
 * resulting external address as a multiaddr. Runs on its own daemon thread, refreshing the mapping
 * before it expires; {@link #stop()} interrupts it and unmaps the ports.
 */
public class PortForwarder {
    private static final Logger LOG = Logging.LOG();
    private static final int LIFETIME_SECONDS = 3600;
    private static final long RETRY_MILLIS = 60_000;

    private final int port;
    private final Consumer<Multiaddr> onMapped;
    private volatile Thread thread;

    public PortForwarder(int port, Consumer<Multiaddr> onMapped) {
        this.port = port;
        this.onMapped = onMapped;
    }

    public synchronized void start() {
        if (thread != null)
            return;
        thread = new Thread(this::run, "port-forwarder-" + port);
        thread.setDaemon(true);
        thread.start();
    }

    public synchronized void stop() {
        if (thread != null) {
            thread.interrupt();
            thread = null;
        }
    }

    private void run() {
        Gateway network = NetworkGateway.create();
        Gateway process = ProcessGateway.create();
        Bus networkBus = network.getBus();
        Bus processBus = process.getBus();
        while (! Thread.currentThread().isInterrupted()) {
            Map<PortMapper, List<MappedPort>> mappings = new HashMap<>();
            try {
                List<PortMapper> mappers = PortMapperFactory.discover(networkBus, processBus);
                if (mappers.isEmpty()) {
                    LOG.fine("No UPnP/NAT-PMP gateway found to forward port " + port);
                    Thread.sleep(RETRY_MILLIS);
                    continue;
                }
                for (PortMapper mapper : mappers) {
                    List<MappedPort> ports = new ArrayList<>();
                    for (PortType type : new PortType[]{PortType.TCP, PortType.UDP}) {
                        MappedPort mapped = mapper.mapPort(type, port, port, LIFETIME_SECONDS);
                        ports.add(mapped);
                        report(type, mapped);
                    }
                    mappings.put(mapper, ports);
                }
                // refresh half-way through the shortest lifetime
                while (! Thread.currentThread().isInterrupted()) {
                    long minLifetime = mappings.values().stream()
                            .flatMap(List::stream)
                            .mapToLong(MappedPort::getLifetime)
                            .min().orElse(LIFETIME_SECONDS);
                    Thread.sleep(Math.max(1, minLifetime / 2) * 1000L);
                    for (Map.Entry<PortMapper, List<MappedPort>> entry : mappings.entrySet()) {
                        List<MappedPort> ports = entry.getValue();
                        for (int i = 0; i < ports.size(); i++) {
                            MappedPort refreshed = entry.getKey().refreshPort(ports.get(i), ports.get(i).getLifetime());
                            ports.set(i, refreshed);
                            report(refreshed.getPortType(), refreshed);
                        }
                    }
                }
            } catch (InterruptedException e) {
                unmap(mappings); // clears the interrupt flag so the bus calls can run, then we exit
                return;
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Port forwarding error for port " + port + ": " + e.getMessage(), e);
                unmap(mappings);
                try {
                    Thread.sleep(RETRY_MILLIS);
                } catch (InterruptedException ie) {
                    return;
                }
            }
        }
    }

    private void report(PortType type, MappedPort mapped) {
        InetAddress ip = mapped.getExternalAddress();
        if (onMapped == null || ip == null)
            return;
        String host = (ip instanceof Inet6Address ? "/ip6/" : "/ip4/") + ip.getHostAddress();
        String suffix = type == PortType.TCP
                ? "/tcp/" + mapped.getExternalPort()
                : "/udp/" + mapped.getExternalPort() + "/quic-v1";
        try {
            onMapped.accept(new Multiaddr(host + suffix));
        } catch (Exception e) {
            LOG.fine("Could not build multiaddr for mapped port: " + host + suffix);
        }
    }

    private void unmap(Map<PortMapper, List<MappedPort>> mappings) {
        Thread.interrupted(); // clear the interrupt so unmap's bus messages can be sent
        for (Map.Entry<PortMapper, List<MappedPort>> entry : mappings.entrySet()) {
            for (MappedPort mapped : entry.getValue()) {
                try {
                    entry.getKey().unmapPort(mapped);
                } catch (Exception e) {
                    // best effort
                }
            }
        }
    }
}
