package org.peergos.blockstore.s3;

import org.peergos.util.Hasher;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

public class S3AdminRequests {


    private static Instant normaliseDate(ZonedDateTime timestamp) {
        return timestamp.withNano(0).withZoneSameInstant(ZoneId.of("UTC")).toInstant();
    }

    public static String asAwsDate(Instant instant) {
        return instant.toString()
                .replaceAll("[:\\-]|\\.\\d{3}", "");
    }

    public static String asAwsDate(ZonedDateTime time) {
        return asAwsDate(normaliseDate(time));
    }

    public static final ThreadLocal<DocumentBuilder> builder =
            new ThreadLocal<>() {
                @Override
                protected DocumentBuilder initialValue() {
                    try {
                        return DocumentBuilderFactory.newInstance().newDocumentBuilder();
                    } catch (ParserConfigurationException exc) {
                        throw new IllegalArgumentException(exc);
                    }
                }
            };

    public static class ObjectMetadata {
        public final String key, etag;
        public final LocalDateTime lastModified;
        public final long size;

        public ObjectMetadata(String key, String etag, LocalDateTime lastModified, long size) {
            this.key = key;
            this.etag = etag;
            this.lastModified = lastModified;
            this.size = size;
        }
    }

    public static class ListObjectsReply {
        public final String prefix;
        public final boolean isTruncated;
        public final List<ObjectMetadata> objects;
        public final Optional<String> continuationToken;

        public ListObjectsReply(String prefix, boolean isTruncated, List<ObjectMetadata> objects, Optional<String> continuationToken) {
            this.prefix = prefix;
            this.isTruncated = isTruncated;
            this.objects = objects;
            this.continuationToken = continuationToken;
        }
    }

    public static CompletableFuture<PresignedUrl> preSignList(String prefix,
                                                              int maxKeys,
                                                              Optional<String> continuationToken,
                                                              ZonedDateTime now,
                                                              String host,
                                                              String region,
                                                              String accessKeyId,
                                                              String s3SecretKey,
                                                              boolean useHttps,
                                                              Hasher h) {
        Map<String, String> extraQueryParameters = new LinkedHashMap<>();
        extraQueryParameters.put("list-type", "2");
        extraQueryParameters.put("max-keys", "" + maxKeys);
        extraQueryParameters.put("fetch-owner", "false");
        extraQueryParameters.put("prefix", prefix);
        continuationToken.ifPresent(t -> extraQueryParameters.put("continuation-token", t));

        Instant normalised = normaliseDate(now);
        S3Request policy = new S3Request("GET", host, "", S3Request.UNSIGNED, Optional.empty(), false, true,
                extraQueryParameters, Collections.emptyMap(), accessKeyId, region, asAwsDate(normalised));
        return S3Request.preSignRequest(policy, "", host, s3SecretKey, useHttps, h);
    }

    public static ListObjectsReply listObjects(String prefix,
                                               int maxKeys,
                                               Optional<String> continuationToken,
                                               ZonedDateTime now,
                                               String host,
                                               String region,
                                               String accessKeyId,
                                               String s3SecretKey,
                                               Function<PresignedUrl, byte[]> getter,
                                               Supplier<DocumentBuilder> builder,
                                               boolean useHttps,
                                               Hasher h) {
        PresignedUrl listReq = preSignList(prefix, maxKeys, continuationToken, now, host, region, accessKeyId, s3SecretKey, useHttps, h).join();
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(getter.apply(listReq));
            if (false) { //debugging code
                String docString = new String(bais.readAllBytes());
                System.currentTimeMillis();
            }
            Document xml = builder.get().parse(bais);
            List<ObjectMetadata> res = new ArrayList<>();
            Node root = xml.getFirstChild();
            NodeList topLevel = root.getChildNodes();
            boolean isTruncated = false;
            Optional<String> nextContinuationToken = Optional.empty();
            for (int t=0; t < topLevel.getLength(); t++) {
                Node top = topLevel.item(t);
                if ("IsTruncated".equals(top.getNodeName())) {
                    String val = top.getTextContent();
                    isTruncated = "true".equals(val);
                }
                if ("NextContinuationToken".equals(top.getNodeName())) {
                    String val = top.getTextContent();
                    nextContinuationToken = Optional.of(val);
                }
                if ("Contents".equals(top.getNodeName())) {
                    NodeList childNodes = top.getChildNodes();
                    String key=null, etag=null, modified=null;
                    long size=0;
                    for (int i = 0; i < childNodes.getLength(); i++) {
                        Node n = childNodes.item(i);
                        if ("Key".equals(n.getNodeName())) {
                            key = n.getTextContent();
                        } else if ("LastModified".equals(n.getNodeName())) {
                            modified = n.getTextContent();
                        } else if ("ETag".equals(n.getNodeName())) {
                            etag = n.getTextContent();
                        } else if ("Size".equals(n.getNodeName())) {
                            size = Long.parseLong(n.getTextContent());
                        }
                    }
                    res.add(new ObjectMetadata(key, etag, LocalDateTime.parse(modified.substring(0, modified.length() - 1)), size));
                }
            }
            return new ListObjectsReply(prefix, isTruncated, res, nextContinuationToken);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
