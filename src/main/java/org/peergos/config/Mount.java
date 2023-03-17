package org.peergos.config;

import org.peergos.util.JsonHelper;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Mount implements Jsonable {
    public final String mountPoint;
    public final String prefix;
    public final String type;
    private Map<String, Object> params;

    public Mount(String mountPoint, String prefix, String type, Map<String, Object> params) {
        this.mountPoint = mountPoint;
        this.prefix = prefix;
        this.type = type;
        this.params = new LinkedHashMap<>(params);
    }
    public Map<String, Object> getParams() {
        return new HashMap<String, Object>(params);
    }

    public Map<String, Object> toJson() {
        Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put("mountpoint", mountPoint);
        configMap.put("prefix", prefix);
        configMap.put("child", params);
        configMap.put("type", type);
        return configMap;
    }
    public static Mount fromJson(Map<String, Object> json) {
        Map<String, Object> params =  JsonHelper.getPropertyMap(json, "child");
        return new Mount(JsonHelper.getStringProperty(json, "mountpoint"),
                JsonHelper.getStringProperty(json, "prefix"),
                JsonHelper.getStringProperty(json, "type"),
                params
        );
    }
}
