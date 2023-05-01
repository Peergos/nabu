package org.peergos.net;

import java.util.Map;

public class ProxyResponse {
    public final byte[] body;
    public final Map<String, String> headers;

    public final int statusCode;

    public ProxyResponse(byte[] body, Map<String, String> headers, int statusCode) {
        this.body = body;
        this.headers = headers;
        this.statusCode = statusCode;
    }
}
