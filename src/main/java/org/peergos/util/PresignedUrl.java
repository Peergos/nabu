package org.peergos.util;

import java.util.Map;

public class PresignedUrl {

    public final String base;
    public final Map<String, String> fields;

    public PresignedUrl(String base, Map<String, String> fields) {
        this.base = base;
        this.fields = fields;
    }
}
