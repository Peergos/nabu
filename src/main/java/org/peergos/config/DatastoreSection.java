package org.peergos.config;

import org.peergos.util.JsonHelper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DatastoreSection implements Jsonable {
    public final Mount blockMount;

    public final Mount rootMount;
    public final Filter filter;

    public DatastoreSection(Mount blockMount, Mount rootMount, Filter filter) {
        this.blockMount = blockMount;
        this.rootMount = rootMount;
        this.filter = filter;
    }

    public Map<String, Object> toJson() {
        Map<String, Object> datastoreMap = new LinkedHashMap<>();
        datastoreMap.put("Filter", filter.toJson());
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
        Optional<Map<String, Object>> filterJsonOpt =  JsonHelper.getOptionalPropertyMap(json, "Datastore", "Filter");
        Filter filter = filterJsonOpt.map( f -> Jsonable.parse(f, p -> Filter.fromJson(p))).orElse(Filter.none());

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
        return new DatastoreSection(blockMountOpt.get(), rootMountOpt.get(), filter);
    }
}
