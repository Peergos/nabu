package org.peergos.blockstore.s3;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.peergos.Hash;
import org.peergos.blockstore.Blockstore;
import org.peergos.blockstore.RateLimitException;
import org.peergos.blockstore.metadatadb.BlockMetadata;
import org.peergos.blockstore.metadatadb.BlockMetadataStore;
import org.peergos.cbor.CborObject;
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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.*;

public class S3Blockstore implements Blockstore {

    private static final Logger LOG = Logging.LOG();

    private static final Histogram readTimerLog = Histogram.build()
            .labelNames("filesize")
            .name("block_read_seconds")
            .help("Time to read a block from immutable storage")
            .exponentialBuckets(0.01, 2, 16)
            .register();
    private static final Histogram writeTimerLog = Histogram.build()
            .labelNames("filesize")
            .name("s3_block_write_seconds")
            .help("Time to write a block to immutable storage")
            .exponentialBuckets(0.01, 2, 16)
            .register();
    private static final Counter blockHeads = Counter.build()
            .name("s3_block_heads")
            .help("Number of block heads to S3")
            .register();
    private static final Counter blockGets = Counter.build()
            .name("s3_block_gets")
            .help("Number of block gets to S3")
            .register();
    private static final Counter failedBlockGets = Counter.build()
            .name("s3_block_get_failures")
            .help("Number of failed block gets to S3")
            .register();
    private static final Counter blockPuts = Counter.build()
            .name("s3_block_puts")
            .help("Number of block puts to S3")
            .register();
    private static final Histogram blockPutBytes = Histogram.build()
            .labelNames("size")
            .name("s3_block_put_bytes")
            .help("Number of bytes written to S3")
            .exponentialBuckets(0.01, 2, 16)
            .register();

    private static final Counter getRateLimited = Counter.build()
            .name("s3_get_rate_limited")
            .help("Number of times we get a http 429 rate limit response during a block get")
            .register();

    private static final Counter rateLimited = Counter.build()
            .name("s3_rate_limited")
            .help("Number of times we get a http 429 rate limit response")
            .register();

    private final String region;
    private final String bucket;
    private final String regionEndpoint;
    private final String accessKeyId;
    private final String secretKey;
    private final String host;
    private final boolean useHttps;
    private final String folder;


    private final Hasher hasher;

    private final BlockMetadataStore blockMetadata;

    public S3Blockstore(Map<String, Object> params, BlockMetadataStore blockMetadata) {
        this.blockMetadata = blockMetadata;
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

    @Override
    public CompletableFuture<BlockMetadata> getBlockMetadata(Cid h) {
        if (h.getType() == Multihash.Type.id)
            return Futures.of(new BlockMetadata(0, CborObject.getLinks(h, h.getHash())));
        Optional<BlockMetadata> cached = blockMetadata.get(h);
        if (cached.isPresent())
            return Futures.of(cached.get());
        Optional<byte[]> data = get(h).join();
        if (data.isEmpty())
            throw new IllegalStateException("Block not present locally: " + h);
        byte[] bloc = data.get();
        if (h.codec == Cid.Codec.Raw) {
            // we should avoid this by populating the metadata store, as it means two S3 calls, a ranged GET and a HEAD
            int size = getSizeWithoutRetry(h).join().get();
            BlockMetadata meta = new BlockMetadata(size, Collections.emptyList());
            blockMetadata.put(h, meta);
            return Futures.of(meta);
        }
        return Futures.of(blockMetadata.put(h, bloc));
    }

    public void updateMetadataStoreIfEmpty() {
        if (blockMetadata.size() > 0)
            return;
        LOG.info("Updating block metadata store from S3. Listing blocks...");
        List<Cid> all = directRefs().join();
        LOG.info("Updating block metadata store from S3. Updating db with " + all.size() + " blocks...");

        int updateParallelism = 10;
        ForkJoinPool pool = new ForkJoinPool(updateParallelism);
        int batchSize = all.size() / updateParallelism;
        AtomicLong progress = new AtomicLong(0);
        int tenth = batchSize/10;

        List<ForkJoinTask<Optional<BlockMetadata>>> futures = IntStream.range(0, updateParallelism)
                .mapToObj(b -> pool.submit(() -> IntStream.range(b * batchSize, (b + 1) * batchSize)
                        .mapToObj(i -> {
                            BlockMetadata res = getBlockMetadata(all.get(i)).join();
                            if (i % (batchSize / 10) == 0) {
                                long updatedProgress = progress.addAndGet(tenth);
                                if (updatedProgress * 10 / all.size() > (updatedProgress - tenth) * 10 / all.size())
                                    LOG.info("Populating block metadata: " + updatedProgress * 100 / all.size() + "% done");
                            }
                            return res;
                        })
                        .reduce((x, y) -> y)))
                .collect(Collectors.toList());
        futures.stream()
                .map(ForkJoinTask::join)
                .collect(Collectors.toList());
        LOG.info("Finished updating block metadata store from S3.");
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
        if (blockMetadata.get(cid).isEmpty())
            return CompletableFuture.completedFuture(false);
        return getWithBackoff(() -> getSizeWithoutRetry(cid)).thenApply(optSize -> optSize.isPresent());
    }

    @Override
    public CompletableFuture<Boolean> hasAny(Multihash h) {
        return Futures.of(Stream.of(Cid.Codec.DagCbor, Cid.Codec.Raw, Cid.Codec.DagProtobuf)
                .anyMatch(c -> has(new Cid(1, c, h.getType(), h.getHash())).join()));
    }

    private CompletableFuture<Optional<Integer>> getSizeWithoutRetry(Cid cid) {
        Optional<BlockMetadata> meta = blockMetadata.get(cid);
        if (meta.isPresent())
            return Futures.of(Optional.of(meta.get().size));
        if (cid.getType() == Multihash.Type.id) // Identity hashes are not actually stored explicitly
            return Futures.of(Optional.of(0));
        try {
            PresignedUrl headUrl = S3Request.preSignHead(folder + hashToKey(cid), Optional.of(60),
                    S3AdminRequests.asAwsDate(ZonedDateTime.now()), host, region, accessKeyId, secretKey, useHttps, hasher).join();
            Map<String, List<String>> headRes = HttpUtil.head(headUrl.base, headUrl.fields);
            blockHeads.inc();
            long size = Long.parseLong(headRes.get("Content-Length").get(0));
            return Futures.of(Optional.of((int)size));
        } catch (FileNotFoundException f) {
            LOG.warning("S3 404 error reading " + cid);
            return Futures.of(Optional.empty());
        } catch (IOException e) {
            String msg = e.getMessage();
            boolean rateLimitedResult = msg.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown</Code>");
            if (rateLimitedResult) {
                rateLimited.inc();
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
        if (blockMetadata.get(cid).isEmpty())
            return CompletableFuture.completedFuture(Optional.empty());
        return getWithBackoff(() -> getWithoutRetry(cid));
    }

    private CompletableFuture<Optional<byte[]>> getWithoutRetry(Cid cid) {
        String path = folder + hashToKey(cid);
        Optional<Pair<Integer, Integer>> range = Optional.empty();
        PresignedUrl getUrl = S3Request.preSignGet(path, Optional.of(600), range,
                S3AdminRequests.asAwsDate(ZonedDateTime.now()), host, region, accessKeyId, secretKey, useHttps, hasher).join();
        Histogram.Timer readTimer = readTimerLog.labels("read").startTimer();
        try {
            byte[] block = HttpUtil.get(getUrl.base, getUrl.fields);
            blockGets.inc();
            blockMetadata.put(cid, block);
            return Futures.of(Optional.of(block));
        } catch (SocketTimeoutException | SSLException e) {
            // S3 can't handle the load so treat this as a rate limit and slow down
            throw new RateLimitException();
        } catch (IOException e) {
            String msg = e.getMessage();
            boolean rateLimitedResult = msg.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown</Code>");
            if (rateLimitedResult) {
                getRateLimited.inc();
                rateLimited.inc();
                throw new RateLimitException();
            }
            boolean notFound = msg.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code>");
            if (! notFound) {
                LOG.warning("S3 error reading " + path);
                LOG.log(Level.WARNING, msg, e);
            }
            failedBlockGets.inc();
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            readTimer.observeDuration();
        }
    }

    @Override
    public CompletableFuture<Cid> put(byte[] block, Cid.Codec codec) {
        return getWithBackoff(() -> putWithoutRetry(block, codec));
    }
    public CompletableFuture<Cid> putWithoutRetry(byte[] block, Cid.Codec codec) {
        Histogram.Timer writeTimer = writeTimerLog.labels("write").startTimer();
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
            HttpUtil.put(putUrl.base, putUrl.fields, block);
            blockMetadata.put(cid, block);
            blockPuts.inc();
            blockPutBytes.labels("size").observe(block.length);
            return CompletableFuture.completedFuture(cid);
        } catch (IOException e) {
            String msg = e.getMessage();
            boolean rateLimitedResult = msg.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown</Code>");
            if (rateLimitedResult) {
                rateLimited.inc();
                throw new RateLimitException();
            }
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            writeTimer.observeDuration();
        }
    }

    @Override
    public CompletableFuture<Boolean> rm(Cid cid) {
        try {
            PresignedUrl delUrl = S3Request.preSignDelete(folder + hashToKey(cid), S3AdminRequests.asAwsDate(ZonedDateTime.now()), host,
                    region, accessKeyId, secretKey, useHttps, hasher).join();
            HttpUtil.delete(delUrl.base, delUrl.fields);
            blockMetadata.remove(cid);
            return CompletableFuture.completedFuture(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<List<Cid>> refs() {
        return CompletableFuture.completedFuture(blockMetadata.list().collect(Collectors.toList()));
    }

    public CompletableFuture<List<Cid>> directRefs() {
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
                                return HttpUtil.get(url.base, url.fields);
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
