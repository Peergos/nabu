package org.peergos.config;

import org.peergos.util.JsonHelper;

import java.util.LinkedHashMap;
import java.util.Map;

public class Filter implements Jsonable {
    public final FilterType type;
    public final Double falsePositiveRate;

    public Filter(FilterType type, Double falsePositiveRate) {
        this(type.type, falsePositiveRate);
    }
    public Filter(String filterType, Double falsePositiveRate) {
        if (falsePositiveRate < 0.0 || falsePositiveRate > 1.0) {
            throw new IllegalStateException("Invalid Filter false positive rate: " + falsePositiveRate);
        }
        this.type = FilterType.lookup(filterType);
        this.falsePositiveRate = falsePositiveRate;
    }
    public static Filter none() {
        return new Filter(FilterType.NONE, 0.0);
    }
    public Map<String, Object> toJson() {
        Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put("type", type.type);
        configMap.put("falsePositiveRate", falsePositiveRate.toString());
        return configMap;
    }
    public static Filter fromJson(Map<String, Object> json) {
        return new Filter(JsonHelper.getStringProperty(json, "type"),
            Double.parseDouble(JsonHelper.getStringProperty(json, "falsePositiveRate"))
        );
    }
}
