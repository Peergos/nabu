package org.peergos.util;

import com.sun.net.httpserver.HttpExchange;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.util.*;
import java.util.logging.Level;

public class HttpUtil {

    public static boolean allowedQuery(HttpExchange exchange) {
        // only allow http POST requests
        if (! exchange.getRequestMethod().equals("POST")) {
            return false;
        }
        return true;
    }

    /** Parse a url query string ignoring encoding
     *
     * @param query
     * @return
     */
    public static Map<String, List<String>> parseQuery(String query) {
        if (query == null)
            return Collections.emptyMap();
        if (query.startsWith("?"))
            query = query.substring(1);
        String[] parts = query.split("&");
        Map<String, List<String>> res = new HashMap<>();
        for (String part : parts) {
            int sep = part.indexOf("=");
            String key = part.substring(0, sep);
            String value = part.substring(sep + 1);
            res.putIfAbsent(key, new ArrayList<>());
            res.get(key).add(value);
        }
        return res;
    }

    public static void replyError(HttpExchange exchange, Throwable t) {
        try {
            Logging.LOG().log(Level.WARNING, t.getMessage(), t);
            Throwable cause = t.getCause();
            if (cause != null)
                exchange.getResponseHeaders().set("Trailer", URLEncoder.encode(cause.getMessage(), "UTF-8"));
            else
                exchange.getResponseHeaders().set("Trailer", URLEncoder.encode(t.getMessage(), "UTF-8"));

            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(400, 0);
        } catch (IOException e) {
            Logging.LOG().log(Level.WARNING, e.getMessage(), e);
        }
    }

    public static FullHttpResponse replyError(Throwable throwable) {
        Logging.LOG().log(Level.WARNING, throwable.getMessage(), throwable);
        FullHttpResponse reply = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, Unpooled.buffer(0));
        Throwable cause = throwable.getCause();
        try {
            if (cause != null)
                reply.headers().set("Trailer", URLEncoder.encode(cause.getMessage(), "UTF-8"));
            else
                reply.headers().set("Trailer", URLEncoder.encode(throwable.getMessage(), "UTF-8"));
        } catch (UnsupportedEncodingException uee) {
        }
        reply.headers().set("Content-Type", "text/plain");
        reply.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
        return reply;
    }
}
