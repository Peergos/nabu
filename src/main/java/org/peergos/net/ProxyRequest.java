package org.peergos.net;

import io.netty.handler.codec.http.HttpMethod;

import java.util.List;
import java.util.Map;

public class ProxyRequest {

    public enum Method {
        OPTIONS, GET, HEAD, POST, PUT, PATCH, DELETE, TRACE, CONNECT
    }
    public final String path;
    public final Method method;
    public final byte[] body;
    public final Map<String, List<String>> headers;
    public final Map<String, List<String>> queryParams;

    public ProxyRequest(String path, Method method, Map<String, List<String>> headers,
                        Map<String, List<String>> queryParams, byte[] body) {
        this.path = path;
        this.method = method;
        this.body = body;
        this.headers = headers;
        this.queryParams = queryParams;
    }
}
