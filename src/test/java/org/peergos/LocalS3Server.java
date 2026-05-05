package org.peergos;

import com.sun.net.httpserver.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class LocalS3Server {
    private final HttpServer server;

    public LocalS3Server(Path storageRoot, String bucket, String accessKey, String secretKey, int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 128);
        server.createContext("/", new LocalS3Handler(storageRoot, bucket, accessKey, secretKey));
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    /**
     * Returns S3Blockstore params wired to this local server.
     * S3Blockstore in non-HTTPS mode (detected via "localhost:" in endpoint) prepends
     * the bucket to all paths, so a block stored as "key" lives at storageRoot/bucket/key.
     */
    public static Map<String, Object> getParams(String bucket, String accessKey, String secretKey, int port) {
        Map<String, Object> params = new HashMap<>();
        params.put("type", "s3ds");
        params.put("region", "us-east-1");
        params.put("bucket", bucket);
        params.put("rootDirectory", "");
        params.put("regionEndpoint", "localhost:" + port);
        params.put("accessKey", accessKey);
        params.put("secretKey", secretKey);
        return params;
    }
}
