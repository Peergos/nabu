package org.peergos.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Config {
    private final Map<String, Object> configuration;
    public Config(Map<String, Object> jsonMap) {
        this.configuration = jsonMap;
    }

    public String getProperty(String... propNames) {
        Map<String, Object> currentMap = traverseProperties(Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            throw new IllegalStateException("Property not found:" + lastProperty);
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof String)) {
            throw new IllegalStateException("Not a String property:" + lastProperty);
        }
        return (String)currentMap.get(lastProperty);
    }
    public List<Map> getPropertyList(String... propNames) {
        Map<String, Object> currentMap = traverseProperties(Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            throw new IllegalStateException("Property not found:" + lastProperty);
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof List)) {
            throw new IllegalStateException("Not a List property:" + lastProperty);
        }
        return (List<Map>)currentMap.get(lastProperty);
    }
    public Map<String, Object> getPropertyMap(String... propNames) {
        Map<String, Object> currentMap = traverseProperties(Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            throw new IllegalStateException("Property not found:" + lastProperty);
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof Map)) {
            throw new IllegalStateException("Not a List property:" + lastProperty);
        }
        return (Map<String, Object>)currentMap.get(lastProperty);
    }
    public Map traverseProperties(List<String> propNames) {
        Map<String, Object> currentMap = configuration;
        for (String propName : propNames) {
            currentMap = (Map<String, Object>)currentMap.get(propName);
            if (currentMap == null) {
                throw new IllegalStateException("Property not found:" + propName);
            }
        }
        return currentMap;
    }
}
