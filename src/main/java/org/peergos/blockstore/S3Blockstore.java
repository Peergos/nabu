package org.peergos.blockstore;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.peergos.Hash;
import org.peergos.util.Hasher;
import org.peergos.util.*;

import javax.net.ssl.SSLException;
import java.io.*;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class S3Blockstore implements Blockstore {

    private static final Logger LOG = Logger.getLogger(S3Blockstore.class.getName());

    private final String region;
    private final String bucket;
    private final String regionEndpoint;
    private final String accessKeyId;
    private final String secretKey;
    private final String host;
    private final boolean useHttps;
    private final String folder;


    private final Hasher hasher;

    public S3Blockstore(Map<String, Object> params) {
        region = getParam(params, "region");
        bucket = getParam(params, "bucket");
        regionEndpoint = getParam(params, "regionEndpoint", "");
        String paramAccessKey = getParam(params, "accessKey", "");
        String paramSecretKey = getParam(params, "secretKey", "");
        if (paramAccessKey.equals("") && paramSecretKey.equals("")) {
            String envAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
            String envSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
            if (envAccessKey != null && envSecretKey != null) {
                accessKeyId = envAccessKey;
                secretKey = envSecretKey;
            } else {
                Map<String, String> credentials = readKeysFromAwsConfig();
                if (envAccessKey != null) {
                    credentials.put("aws_access_key_id", envAccessKey);
                }
                if (envSecretKey != null) {
                    credentials.put("aws_secret_access_key", envSecretKey);
                }
                accessKeyId = credentials.get("aws_access_key_id");
                secretKey = credentials.get("aws_secret_access_key");
            }
        } else {
            accessKeyId = paramAccessKey;
            secretKey = paramSecretKey;
        }
        host = getHost();
        useHttps = useHttps();
        String rootDirectory = getParam(params, "rootDirectory");
        folder = (useHttps ? "" : bucket + "/") + (rootDirectory.length() == 0 || rootDirectory.endsWith("/") ?
                rootDirectory : rootDirectory + "/");

        hasher = new Hasher();
        LOG.info("Using S3BlockStore");
    }
    private String getHost() {
        return bucket + "." + regionEndpoint;
    }
    private boolean useHttps() {
        String host = getHost();
        return ! host.endsWith("localhost") && ! host.contains("localhost:");
    }
    private Map<String, String> readKeysFromAwsConfig() {
        String filePath = System.getenv("HOME") + "/" + ".aws/credentials";
        LOG.info("Reading [default] config from: " + filePath);
        Map<String, String> params = new HashMap<>();
        try {
            List<String> lines = Files.readAllLines(Path.of(filePath));
            boolean foundDefaultSection = false;
            boolean foundAccessKey = false;
            boolean foundSecretKey = false;
            for(String line : lines) {
                String trimmedLine = line.trim();
                if(!foundDefaultSection) {
                    if (trimmedLine.equals("[default]")){
                        foundDefaultSection = true;
                    }
                } else {
                    if (trimmedLine.startsWith("[")) {
                        throw new IllegalStateException("Unable to read expected fields in: " + filePath);
                    } else if(trimmedLine.startsWith("aws_access_key_id")) {
                        params.put("aws_access_key_id", extractParamValue(trimmedLine));
                        foundAccessKey = true;
                    } else if(trimmedLine.startsWith("aws_secret_access_key")) {
                        params.put("aws_secret_access_key", extractParamValue(trimmedLine));
                        foundSecretKey = true;
                    }
                }

            }
            if (!foundDefaultSection) {
                throw new IllegalStateException("Unable to find [default] in: " + filePath);
            }
            if (!foundAccessKey) {
                throw new IllegalStateException("Unable to find aws_access_key_id");
            }
            if (!foundSecretKey) {
                throw new IllegalStateException("Unable to find aws_secret_access_key");
            }
        } catch (IOException ioe) {
            throw new IllegalStateException("Unable to read: "  + filePath, ioe);
        }
        return params;
    }
    private String extractParamValue(String line) {
        int equalsIndex = line.indexOf("=");
        if (equalsIndex == -1) {
            throw new IllegalStateException("Unable to read line: " + line);
        }
        return line.substring(equalsIndex + 1).trim();
    }
    private String getParam(Map<String, Object> params, String key) {
        if (params.containsKey(key)) {
            return ((String )params.get(key)).trim();
        } else {
            throw new IllegalStateException("Expecting param: " + key);
        }
    }
    private String getParam(Map<String, Object> params, String key, String defaultValue) {
        if (params.containsKey(key)) {
            return ((String )params.get(key)).trim();
        } else {
            return defaultValue;
        }
    }

    private static <V> V getWithBackoff(Supplier<V> req) {
        long sleep = 100;
        for (int i=0; i < 20; i++) {
            try {
                return req.get();
            } catch (RateLimitException e) {
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException f) {}
                sleep *= 2;
            }
        }
        throw new IllegalStateException("Couldn't process request because of rate limit!");
    }

    @Override
    public CompletableFuture<Boolean> has(Cid cid) {
            return getWithBackoff(() -> getSizeWithoutRetry(cid)).thenApply(optSize -> optSize.isPresent());
    }

    private CompletableFuture<Optional<Integer>> getSizeWithoutRetry(Cid cid) {
        try {
            PresignedUrl headUrl = S3Request.preSignHead(folder + hashToKey(cid), Optional.of(60),
                    S3AdminRequests.asAwsDate(ZonedDateTime.now()), host, region, accessKeyId, secretKey, useHttps, hasher).join();
            Map<String, List<String>> headRes = HttpUtil.head(headUrl);
            long size = Long.parseLong(headRes.get("Content-Length").get(0));
            return Futures.of(Optional.of((int)size));
        } catch (FileNotFoundException f) {
            LOG.warning("S3 404 error reading " + cid);
            return Futures.of(Optional.empty());
        } catch (IOException e) {
            String msg = e.getMessage();
            boolean rateLimited = msg.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown</Code>");
            if (rateLimited) {
                throw new RateLimitException();
            }
            boolean notFound = msg.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code>");
            if (! notFound) {
                LOG.warning("S3 error reading " + cid);
                LOG.log(Level.WARNING, msg, e);
            }
            return Futures.of(Optional.empty());
        }
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid cid) {
        return getWithBackoff(() -> getWithoutRetry(cid));
    }

    private CompletableFuture<Optional<byte[]>> getWithoutRetry(Cid cid) {
        String path = folder + hashToKey(cid);
        Optional<Pair<Integer, Integer>> range = Optional.empty();
        PresignedUrl getUrl = S3Request.preSignGet(path, Optional.of(600), range,
                S3AdminRequests.asAwsDate(ZonedDateTime.now()), host, region, accessKeyId, secretKey, useHttps, hasher).join();
        try {
            byte[] block = HttpUtil.get(getUrl);
            return Futures.of(Optional.of(block));
        } catch (SocketTimeoutException | SSLException e) {
            // S3 can't handle the load so treat this as a rate limit and slow down
            throw new RateLimitException();
        } catch (IOException e) {
            String msg = e.getMessage();
            boolean rateLimited = msg.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown</Code>");
            if (rateLimited) {
                throw new RateLimitException();
            }
            boolean notFound = msg.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code>");
            if (! notFound) {
                LOG.warning("S3 error reading " + path);
                LOG.log(Level.WARNING, msg, e);
            }
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public CompletableFuture<Cid> put(byte[] block, Cid.Codec codec) {
        return getWithBackoff(() -> putWithoutRetry(block, codec));
    }
    public CompletableFuture<Cid> putWithoutRetry(byte[] block, Cid.Codec codec) {
        byte[] hash = Hash.sha256(block);
        Cid cid = new Cid(1, codec, Multihash.Type.sha2_256, hash);
        String key = hashToKey(cid);
        try {
            String s3Key = folder + key;
            Map<String, String> extraHeaders = new TreeMap<>();
            extraHeaders.put("Content-Type", "application/octet-stream");
            String contentHash =  ArrayOps.bytesToHex(hash);
            PresignedUrl putUrl = S3Request.preSignPut(s3Key, block.length, contentHash, false,
                    S3AdminRequests.asAwsDate(ZonedDateTime.now()), host, extraHeaders, region, accessKeyId, secretKey, useHttps, hasher).join();
            HttpUtil.put(putUrl, block);
            return CompletableFuture.completedFuture(cid);
        } catch (IOException e) {
            String msg = e.getMessage();
            boolean rateLimited = msg.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown</Code>");
            if (rateLimited) {
                throw new RateLimitException();
            }
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public CompletableFuture<Boolean> rm(Cid cid) {
        try {
            PresignedUrl delUrl = S3Request.preSignDelete(folder + hashToKey(cid), S3AdminRequests.asAwsDate(ZonedDateTime.now()), host,
                    region, accessKeyId, secretKey, useHttps, hasher).join();
            HttpUtil.delete(delUrl);
            return CompletableFuture.completedFuture(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        //not implemented
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<List<Cid>> refs() {
        List<Cid> cidList = new ArrayList<>();
        applyToAll(obj -> cidList.add(keyToHash(obj.key)), Integer.MAX_VALUE);
        return CompletableFuture.completedFuture(cidList);
    }

    private void applyToAll(Consumer<S3AdminRequests.ObjectMetadata> processor, long maxObjects) {
        try {
            Optional<String> continuationToken = Optional.empty();
            S3AdminRequests.ListObjectsReply result;
            long processedObjects = 0;
            do {
                result = S3AdminRequests.listObjects(folder, 1_000, continuationToken,
                        ZonedDateTime.now(), host, region, accessKeyId, secretKey, url -> {
                            try {
                                return HttpUtil.get(url);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }, S3AdminRequests.builder::get, useHttps, hasher);

                for (S3AdminRequests.ObjectMetadata objectSummary : result.objects) {
                    if (objectSummary.key.endsWith("/")) {
                        LOG.fine(" - " + objectSummary.key + "  " + "(directory)");
                        continue;
                    }
                    processor.accept(objectSummary);
                    processedObjects++;
                    if (processedObjects >= maxObjects)
                        return;
                }
                LOG.log(Level.FINE, "Next Continuation Token : " + result.continuationToken);
                continuationToken = result.continuationToken;
            } while (result.isTruncated);

        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
