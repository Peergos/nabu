package org.peergos.net;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.peergos.util.Exceptions;
import org.peergos.util.HttpUtil;
import org.peergos.util.Logging;

import java.io.DataOutputStream;
import java.io.IOException;
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
                httpExchange.sendResponseHeaders(403, 0);
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
