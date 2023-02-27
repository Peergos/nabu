package org.peergos.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Config {
    private final Map configuration;
    public Config(Map jsonMap) {
        this.configuration = jsonMap;
    }

    public String getProperty(String... propNames) {
        Map currentMap = traverseProperties(Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
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
    public List getPropertyList(String... propNames) {
        Map currentMap = traverseProperties(Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            throw new IllegalStateException("Property not found:" + lastProperty);
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof List)) {
            throw new IllegalStateException("Not a List property:" + lastProperty);
        }
        return (List)currentMap.get(lastProperty);
    }
    public Map getPropertyMap(String... propNames) {
        Map currentMap = traverseProperties(Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            throw new IllegalStateException("Property not found:" + lastProperty);
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof Map)) {
            throw new IllegalStateException("Not a List property:" + lastProperty);
        }
        return (Map)currentMap.get(lastProperty);
    }
    public Map traverseProperties(List<String> propNames) {
        Map currentMap = configuration;
        for (String propName : propNames) {
            currentMap = (Map)currentMap.get(propName);
            if (currentMap == null) {
                throw new IllegalStateException("Property not found:" + propName);
            }
        }
        return currentMap;
    }
}
