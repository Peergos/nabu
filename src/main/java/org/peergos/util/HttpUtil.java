package org.peergos.util;

import com.sun.net.httpserver.HttpExchange;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.peergos.blockstore.RateLimitException;
import org.peergos.blockstore.s3.PresignedUrl;

import java.io.*;
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
            if (cause != null && cause.getMessage() != null)
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

    public static Map<String, List<String>> head(String uri, Map<String, String> fields) throws IOException {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URI(uri).toURL().openConnection();
            conn.setRequestMethod("HEAD");
            for (Map.Entry<String, String> e : fields.entrySet()) {
                conn.setRequestProperty(e.getKey(), e.getValue());
            }

            try {
                int respCode = conn.getResponseCode();
                if (respCode == 200)
                    return conn.getHeaderFields();
                if (respCode == 503)
                    throw new RateLimitException();
                if (respCode == 404)
                    throw new FileNotFoundException();
                throw new IllegalStateException("HTTP " + respCode);
            } catch (IOException e) {
                InputStream err = conn.getErrorStream();
                if (err == null)
                    throw e;
                byte[] errBody = readFully(err);
                throw new IOException(new String(errBody));
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] get(String uri, Map<String, String> fields) throws IOException {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URI(uri).toURL().openConnection();
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(60_000);
            conn.setRequestMethod("GET");
            for (Map.Entry<String, String> e : fields.entrySet()) {
                conn.setRequestProperty(e.getKey(), e.getValue());
            }

            try {
                int respCode = conn.getResponseCode();
                if (respCode == 503)
                    throw new RateLimitException();
                if (respCode == 404)
                    throw new FileNotFoundException();
                InputStream in = conn.getInputStream();
                return readFully(in);
            } catch (IOException e) {
                InputStream err = conn.getErrorStream();
                if (err == null)
                    throw e;
                byte[] errBody = readFully(err);
                throw new IOException(new String(errBody), e);
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static void delete(String uri, Map<String, String> fields) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URI(uri).toURL().openConnection();
        conn.setRequestMethod("DELETE");
        for (Map.Entry<String, String> e : fields.entrySet()) {
            conn.setRequestProperty(e.getKey(), e.getValue());
        }

        try {
            int code = conn.getResponseCode();
            if (code == 204)
                return;
            InputStream in = conn.getInputStream();
            ByteArrayOutputStream resp = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int r;
            while ((r = in.read(buf)) >= 0)
                resp.write(buf, 0, r);
            throw new IllegalStateException("HTTP " + code + "-" + new String(resp.toByteArray()));
        } catch (IOException e) {
            InputStream err = conn.getErrorStream();
            ByteArrayOutputStream resp = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int r;
            while ((r = err.read(buf)) >= 0)
                resp.write(buf, 0, r);
            throw new IllegalStateException(new String(resp.toByteArray()), e);
        }
    }

    public static byte[] put(String uri, Map<String, String> fields, byte[] body) throws IOException {
        return putOrPost("PUT", uri, fields, body);
    }

    public static byte[] post(String uri, Map<String, String> fields, byte[] body) throws IOException {
        return putOrPost("POST", uri, fields, body);
    }

    private static byte[] putOrPost(String method, String uri, Map<String, String> fields, byte[] body) throws IOException {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URI(uri).toURL().openConnection();
            conn.setRequestMethod(method);
            for (Map.Entry<String, String> e : fields.entrySet()) {
                conn.setRequestProperty(e.getKey(), e.getValue());
            }
            conn.setDoOutput(true);
            OutputStream out = conn.getOutputStream();
            out.write(body);
            out.flush();
            out.close();

            int httpCode = conn.getResponseCode();
            if (httpCode == 503)
                throw new RateLimitException();
            InputStream in = conn.getInputStream();
            return readFully(in);
        } catch (IOException e) {
            if (conn != null) {
                InputStream err = conn.getErrorStream();
                byte[] errBody = readFully(err);
                throw new IOException(new String(errBody));
            }
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] readFully(InputStream in) throws IOException {
        ByteArrayOutputStream bout =  new ByteArrayOutputStream();
        byte[] b =  new  byte[0x1000];
        int nRead;
        while ((nRead = in.read(b, 0, b.length)) != -1 )
            bout.write(b, 0, nRead);
        in.close();
        return bout.toByteArray();
    }
}
