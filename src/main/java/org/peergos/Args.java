package org.peergos;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Args {

    private final Map<String, String> params;

    public Args(Map<String, String> params) {
        this.params = params;
    }

    public List<String> getAllArgs() {
        Map<String, String> env = System.getenv();
        return params.entrySet().stream()
                .filter(e -> ! env.containsKey(e.getKey()))
                .flatMap(e -> Stream.of("-" + e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    public String getArg(String param, String def) {
        if (!params.containsKey(param))
            return def;
        return params.get(param);
    }

    public String getArg(String param) {
        if (!params.containsKey(param))
            throw new IllegalStateException("No parameter: " + param);
        return params.get(param);
    }

    public Optional<String> getOptionalArg(String param) {
        return Optional.ofNullable(params.get(param));
    }

    public Args setArg(String param, String value) {
        Map<String, String> newParams = paramMap();
        newParams.putAll(params);
        newParams.put(param, value);
        return new Args(newParams);
    }

    public boolean hasArg(String arg) {
        return params.containsKey(arg);
    }

    public boolean getBoolean(String param, boolean def) {
        if (!params.containsKey(param))
            return def;
        return "true".equals(params.get(param));
    }

    public boolean getBoolean(String param) {
        if (!params.containsKey(param))
            throw new IllegalStateException("Missing parameter for " + param);
        return "true".equals(params.get(param));
    }

    public int getInt(String param, int def) {
        if (!params.containsKey(param))
            return def;
        return Integer.parseInt(params.get(param));
    }

    public int getInt(String param) {
        if (!params.containsKey(param))
            throw new IllegalStateException("No parameter: " + param);
        return Integer.parseInt(params.get(param));
    }

    public Args setIfAbsent(String key, String value) {
        if (params.containsKey(key))
            return this;
        return setArg(key, value);
    }

    public static Args parse(String[] args) {
        Map<String, String> map = paramMap();
        map.putAll(System.getenv());
        for (int i = 0; i < args.length; i++) {
            String argName = args[i];
            if (argName.startsWith("-"))
                argName = argName.substring(1);

            if ((i == args.length - 1) || args[i + 1].startsWith("-"))
                map.put(argName, "true");
            else
                map.put(argName, args[++i]);
        }
        return new Args(map);
    }

    private static <K, V> Map<K, V> paramMap() {
        return new LinkedHashMap<>(16, 0.75f, false);
    }
}
