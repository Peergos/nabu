package org.peergos.config;

import java.util.Map;
import java.util.TreeMap;

public enum FilterType {
    NONE("none"),
    BLOOM("bloom"),
    INFINI("infini");

    public final String type;
    FilterType(String type) {
        this.type = type;
    }

    private static Map<String, FilterType> lookup = new TreeMap<>();
    static {
        for (FilterType b: FilterType.values())
            lookup.put(b.type, b);
    }

    public static FilterType lookup(String p) {
        if (!lookup.containsKey(p))
            throw new IllegalStateException("Unknown Filter type: " + p);
        return lookup.get(p);
    }
}