package org.peergos.config;

import org.peergos.util.JsonHelper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DatastoreSection {
    public final Mount blockMount;

    public final Mount rootMount;
    public final int bloomFilterSize;

    public DatastoreSection(Mount blockMount, Mount rootMount, int bloomFilterSize) {
        this.blockMount = blockMount;
        this.rootMount = rootMount;
        this.bloomFilterSize = bloomFilterSize;
    }

    public Map<String, Object> toJson() {
        Map<String, Object> datastoreMap = new LinkedHashMap<>();
        datastoreMap.put("BloomFilterSize", bloomFilterSize);

        List<Map<String, Object>> list = List.of(blockMount.toJson(), rootMount.toJson());
        Map<String, Object> specMap = new LinkedHashMap<>();
        specMap.put("mounts", list);
        specMap.put("type", "mount");
        datastoreMap.put("Spec", specMap);
        Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put("Datastore", datastoreMap);
        return configMap;
    }

    public static DatastoreSection fromJson(Map<String, Object> json) {
        Integer size = JsonHelper.getIntProperty(json, "Datastore", "BloomFilterSize");
        String type = JsonHelper.getStringProperty(json, "Datastore", "Spec", "type");
        List<Map<String, Object>> mounts = JsonHelper.getPropertyObjectList(json, "Datastore", "Spec", "mounts");
        List<Mount> mountList = mounts.stream().map(m -> Jsonable.parse(m, p -> Mount.fromJson(p))).collect(Collectors.toList());
        Optional<Mount> blockMountOpt = mountList.stream().filter(m -> m.mountPoint.equals("/blocks")).findFirst();
        Optional<Mount> rootMountOpt = mountList.stream().filter(m -> m.mountPoint.equals("/")).findFirst();
        if (!blockMountOpt.isPresent() || !rootMountOpt.isPresent()) {
            throw new IllegalStateException("Expecting a '/blocks' and '/' mount");
        }
        if (!type.equals("mount")) {
            throw new IllegalStateException("Expecting Datastore/Spec/type == 'mount'");
        }
        return new DatastoreSection(blockMountOpt.get(), rootMountOpt.get(), size);
    }
}
