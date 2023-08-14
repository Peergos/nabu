package org.peergos.util;

import java.util.*;
import java.util.stream.Collectors;

public class JsonHelper {
    public static String getStringProperty(Map<String, Object> kvMap, String... propNames) {
        Map<String, Object> currentMap = traverseProperties(kvMap, Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            throw new IllegalStateException("Property not found: " + lastProperty);
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof String)) {
            throw new IllegalStateException("Not a String property: " + lastProperty);
        }
        return (String)currentMap.get(lastProperty);
    }
    public static Integer getIntProperty(Map<String, Object> kvMap, String... propNames) {
        Map<String, Object> currentMap = traverseProperties(kvMap, Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            throw new IllegalStateException("Property not found: " + lastProperty);
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof Integer)) {
            throw new IllegalStateException("Not an Integer property: " + lastProperty);
        }
        return (Integer)currentMap.get(lastProperty);
    }

    public static Boolean getBooleanProperty(Map<String, Object> kvMap, String... propNames) {
        Map<String, Object> currentMap = traverseProperties(kvMap, Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            throw new IllegalStateException("Property not found: " + lastProperty);
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof Boolean)) {
            throw new IllegalStateException("Not a Boolean property: " + lastProperty);
        }
        return (Boolean)currentMap.get(lastProperty);
    }

    public static List<Object> getPropertyList(Map<String, Object> kvMap, String... propNames) {
        Map<String, Object> currentMap = traverseProperties(kvMap, Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            throw new IllegalStateException("Property not found: " + lastProperty);
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof List)) {
            throw new IllegalStateException("Not a List property: " + lastProperty);
        }
        return (List)currentMap.get(lastProperty);
    }
    public static Optional<List<Object>> getOptionalPropertyList(Map<String, Object> kvMap, String... propNames) {
        Map<String, Object> currentMap = traverseProperties(kvMap, Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            return Optional.empty();
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof List)) {
            throw new IllegalStateException("Not a List property: " + lastProperty);
        }
        return Optional.of((List)currentMap.get(lastProperty));
    }
    public static Optional<Object> getOptionalProperty(Map<String, Object> kvMap, String... propNames) {
        if (!findProperty(kvMap, Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()))) {
            return Optional.empty();
        }
        String lastProperty = propNames[propNames.length -1];
        Map<String, Object> currentMap = traverseProperties(kvMap, Arrays.stream(propNames).limit(propNames.length -1)
                .collect(Collectors.toList()));
        if(! currentMap.containsKey(lastProperty)) {
            return Optional.empty();
        }
        Object obj = currentMap.get(lastProperty);
        return Optional.of(obj);
    }

    public static List<Map<String, Object>> getPropertyObjectList(Map<String, Object> kvMap, String... propNames) {
        Map<String, Object> currentMap = traverseProperties(kvMap, Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            throw new IllegalStateException("Property not found: " + lastProperty);
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof List)) {
            throw new IllegalStateException("Not a List property: " + lastProperty);
        }
        return (List<Map<String, Object>>)currentMap.get(lastProperty);
    }
    public static Map<String, Object> getPropertyMap(Map<String, Object> kvMap, String... propNames) {
        Map<String, Object> currentMap = traverseProperties(kvMap, Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            throw new IllegalStateException("Property not found: " + lastProperty);
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof Map)) {
            throw new IllegalStateException("Not a List property: " + lastProperty);
        }
        return (Map<String, Object>)currentMap.get(lastProperty);
    }
    public static Optional<Map<String, Object>> getOptionalPropertyMap(Map<String, Object> kvMap, String... propNames) {
        Map<String, Object> currentMap = traverseProperties(kvMap, Arrays.stream(propNames).limit(propNames.length -1).collect(Collectors.toList()));
        String lastProperty = propNames[propNames.length -1];
        if(! currentMap.containsKey(lastProperty)) {
            return Optional.empty();
        }
        Object obj = currentMap.get(lastProperty);
        if (! (obj instanceof Map)) {
            throw new IllegalStateException("Not a List property: " + lastProperty);
        }
        return Optional.of((Map<String, Object>)currentMap.get(lastProperty));
    }
    public static Map<String, Object> traverseProperties(Map<String, Object> kvMap, List<String> propNames) {
        Map<String, Object> currentMap = kvMap;
        for (String propName : propNames) {
            currentMap = (Map<String, Object>)currentMap.get(propName);
            if (currentMap == null) {
                throw new IllegalStateException("Property not found: " + propName);
            }
        }
        return currentMap;
    }
    private static boolean findProperty(Map<String, Object> kvMap, List<String> propNames) {
        Map<String, Object> currentMap = kvMap;
        for (String propName : propNames) {
            currentMap = (Map<String, Object>)currentMap.get(propName);
            if (currentMap == null) {
                return false;
            }
        }
        return true;
    }


    public static String pretty(Map<String, Object> kvMap) {
        //N.B. sufficient for our needs.
        String json = JSONParser.toString(kvMap);
        String formatted = json.replaceAll("\\{\"", "{\n\"")
                .replaceAll("}}", "\n}\n}")
                .replaceAll("}}", "}\n}")
                .replaceAll("},\\{", "\n},\n\\{")
                .replaceAll("\\[\"", "[\n\"")
                .replaceAll("\\]\\}", "]\n}")
                .replaceAll("\\}\\]", "\n}]")
                .replaceAll("\"\\]", "\"\n]")
                .replaceAll("\\[\\]", "[\n]")
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
        }
        return sb.toString();
    }

    private static String padStart(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append("\t");
        }
        return sb.toString();
    }
}
