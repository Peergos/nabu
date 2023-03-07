package org.peergos.config;

import java.util.Map;
import java.util.function.Function;

public interface Jsonable {

    Map<String, Object> toJson();

    static <T> T parse(Map<String, Object> in, Function<Map<String, Object>, T> parser) {
        return parser(parser).apply(in);
    }

    private static <T> Function<Map<String, Object>, T> parser(Function<Map<String, Object>, T> parser) {
        return arr -> parser.apply(arr);
    }

}
