package org.peergos.config;

import io.ipfs.cid.Cid;
import org.peergos.util.JsonHelper;

import java.util.*;
import java.util.stream.Collectors;

public class CodecSet implements Jsonable {
    public final Set<Cid.Codec> codecs;
    public CodecSet(Set<Cid.Codec> codecs) {
        this.codecs = new HashSet<>(codecs);
    }
    public static CodecSet empty() {
        return new CodecSet(Collections.emptySet());
    }
    public Map<String, Object> toJson() {
        Map<String, Object> configMap = new LinkedHashMap<>();
        List<String> allowedCodecList = codecs.stream().map(b -> b.toString()).collect(Collectors.toList());
        configMap.put("Codecs", allowedCodecList);
        return configMap;
    }
    public static CodecSet fromJson(Map<String, Object> json) {
        Optional<List<Object>> codecsOpt = JsonHelper.getOptionalPropertyList(json, "Datastore", "Codecs");
        CodecSet codecs = codecsOpt.isEmpty() ? CodecSet.empty()
                : new CodecSet(codecsOpt.get()
                .stream()
                .map(obj -> Cid.Codec.valueOf((String) obj))
                .collect(Collectors.toSet()));
        return codecs;
    }
}
