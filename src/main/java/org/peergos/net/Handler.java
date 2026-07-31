package org.peergos.net;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.peergos.util.Exceptions;
import org.peergos.util.HttpUtil;
import org.peergos.util.Logging;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class Handler implements HttpHandler {

    private static final Logger LOG = Logging.LOG();

    public Handler() {

    }

    abstract void handleCallToAPI(HttpExchange httpExchange);

    @Override
    public void handle(HttpExchange httpExchange) {
        try {
            if (!HttpUtil.allowedQuery(httpExchange)) {
                // -1 means no response body. A length of 0 starts a chunked response that we never
                // terminate, so the caller waits on the connection until it gives up - one parked
                // connection per stray GET from a browser, health check or scanner.
                httpExchange.sendResponseHeaders(403, -1);
                httpExchange.close();
            } else {
                handleCallToAPI(httpExchange);
            }
        } catch (Exception e) {
            Throwable t = Exceptions.getRootCause(e);
            LOG.severe("Error handling " + httpExchange.getRequestURI());
            LOG.log(Level.WARNING, t.getMessage(), t);
            HttpUtil.replyError(httpExchange, t);
        }
    }

    protected static byte[] read(InputStream in) throws IOException {
        try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
             OutputStream gout = new DataOutputStream(bout)) {
            byte[] tmp = new byte[4096];
            int r;
            while ((r = in.read(tmp)) >= 0)
                gout.write(tmp, 0, r);
            in.close();
            return bout.toByteArray();
        }
    }

    protected static void replyJson(HttpExchange exchange, String json) {
        try {
            byte[] raw = json.getBytes();
            exchange.sendResponseHeaders(200, raw.length);
            DataOutputStream dout = new DataOutputStream(exchange.getResponseBody());
            dout.write(raw);
            dout.flush();
            dout.close();
        } catch (IOException e)
        {
            LOG.log(Level.WARNING, e.getMessage(), e);
        }
    }

    protected static void replyBytes(HttpExchange exchange, byte[] body) {
        try {
            exchange.sendResponseHeaders(200, body.length);
            DataOutputStream dout = new DataOutputStream(exchange.getResponseBody());
            dout.write(body);
            dout.flush();
            dout.close();
        } catch (IOException e)
        {
            LOG.log(Level.WARNING, e.getMessage(), e);
        }
    }
}
