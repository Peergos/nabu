package org.peergos;

import io.prometheus.client.Counter;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * A wrapper around the prometheus metrics and HTTP exporter.
 */
public class AggregatedMetrics {
    private static final Logger LOG = Logger.getLogger(Nabu.class.getName());

    private static Counter build(String name, String help) {
        return Counter.build()
                .name(name).help(help).register();
    }

    public static final Counter API_ID  = build("api_id", "Total calls to id.");
    public static final Counter API_VERSION  = build("api_version", "Total calls to version.");
    public static final Counter API_BLOCK_GET  = build("api_block_get", "Total calls to block/get.");
    public static final Counter API_BLOCK_PUT  = build("api_block_put", "Total calls to block/put.");
    public static final Counter API_BLOCK_RM  = build("api_block_rm", "Total calls to block/rm.");
    public static final Counter API_BLOCK_STAT  = build("api_block_stat", "Total calls to block/stat.");
    public static final Counter API_REFS_LOCAL  = build("api_refs_local", "Total calls to refs/local.");
    public static final Counter API_BLOCK_HAS  = build("api_block_has", "Total calls to block/has.");
    public static final Counter API_BLOOM_ADD  = build("api_bloom_add", "Total calls to bloom/add.");
    public static final Counter API_FIND_PROVS  = build("api_dht_findprovs", "Total calls to dht/findprovs.");

    public static void startExporter(String address, int port) throws IOException {
        LOG.info("Starting metrics server at " + address + ":" + port);
        HTTPServer server = new HTTPServer(address, port);
        //shutdown hook on signal
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.close()));
    }
}

