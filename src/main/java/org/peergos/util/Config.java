package org.peergos.util;

import java.util.*;
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

    public Optional<Object> getOptionalProperty(String... propNames) {
        if (!findProperty(Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()))) {
            return Optional.empty();
        }
        String lastProperty = propNames[propNames.length -1];
        Map<String, Object> currentMap = traverseProperties(Arrays.stream(propNames).limit(propNames.length -1)
                .collect(Collectors.toList()));
        if(! currentMap.containsKey(lastProperty)) {
            return Optional.empty();
        }
        Object obj = currentMap.get(lastProperty);
        return Optional.of(obj);
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
    private boolean findProperty(List<String> propNames) {
        Map<String, Object> currentMap = configuration;
        for (String propName : propNames) {
            currentMap = (Map<String, Object>)currentMap.get(propName);
            if (currentMap == null) {
                return false;
            }
        }
        return true;
    }
    @Override
    public String toString() {
        return JSONParser.toString(configuration);
    }

    public String prettyPrint() {
        //N.B. sufficient for our needs.
        String json = JSONParser.toString(configuration);
        String formatted = json.replaceAll("\\{\"", "{\n\"")
                .replaceAll("}}", "\n}\n}")
                .replaceAll("}}", "}\n}")
                .replaceAll("},\\{", "\n},\n\\{")
                .replaceAll("\\[\"", "[\n\"")
                .replaceAll("\\]\\}", "]\n}")
                .replaceAll("\\}\\]", "\n}]")
                .replaceAll("\"\\]", "\"\n]")
                .replaceAll(",\"", ",\n\"");
        StringTokenizer st = new StringTokenizer(formatted, "\n");
        StringBuilder sb = new StringBuilder();
        int indent = 0;
        while(st.hasMoreTokens()) {
            String token = st.nextToken();
            if(token.endsWith("}") || token.endsWith("]")) {
                indent--;
                sb.append(padStart(indent));
                sb.append(token + "\n");
            } else if (token.endsWith("}],")) {
                if (token.length() > 3) {
                    sb.append(padStart(indent));
                    sb.append(token.substring(0, token.length() - 3) + "\n");
                }
                indent--;
                sb.append(padStart(indent));
                sb.append(token.substring(token.length() -3));
            } else if (token.endsWith("},") || token.endsWith("],")) {
                if (token.length() > 2) {
                    sb.append(padStart(indent));
                    sb.append(token.substring(0, token.length() - 2) + "\n");
                }
                indent--;
                sb.append(padStart(indent));
                sb.append(token.substring(token.length() -2));
            } else {
                sb.append(padStart(indent));
                sb.append(token);
            }
            if (token.endsWith("{") || token.endsWith("[")) {
                indent++;
                sb.append("\n");
            } else if(token.endsWith(",") || token.endsWith("\"")) {
                sb.append("\n");
            }
            System.currentTimeMillis();
        }
        return sb.toString();
    }

    public static String padStart(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append("\t");
        }
        return sb.toString();
    }

}
