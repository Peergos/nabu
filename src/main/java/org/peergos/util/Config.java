package org.peergos.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
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

    @Override
    public String toString() {
        return JSONParser.toString(configuration);
    }

    public String prettyPrint() {
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
