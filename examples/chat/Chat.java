package chat;

import io.libp2p.core.Host;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.peergos.HostBuilder;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.protocol.dht.RamProviderStore;
import org.peergos.protocol.dht.RamRecordStore;
import org.peergos.protocol.http.HttpProtocol;

import java.nio.charset.Charset;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

public class Chat {

    public Chat() {

        FullHttpResponse replyOk = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.buffer(0));
        replyOk.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);

        HttpProtocol.Binding node1Http = new HttpProtocol.Binding((s, req, h) -> {
            if (req instanceof FullHttpRequest) {
                ByteBuf content = ((FullHttpRequest) req).content();
                CharSequence contents = content.getCharSequence(0, content.readableBytes(), Charset.defaultCharset());
                String output = contents.toString();
                System.out.println("received msg:" + output);
            }
            h.accept(replyOk.retain());
        });

        int portNumber = 10000 + new Random().nextInt(50000);

        HostBuilder builder = HostBuilder.create(portNumber,
                        new RamProviderStore(),
                        new RamRecordStore(),
                        new RamBlockstore(),
                        (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(node1Http);

        Host node = builder.build();
        node.start().join();
        Thread shutdownHook = new Thread(() -> {
            try {
                node.stop();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        Multiaddr address1 = node.listenAddresses().get(0);
        System.out.println("My address=" + address1);

        System.out.println("Enter address of other node:");
        Scanner in = new Scanner(System.in);
        String address2 = in.nextLine();
        if (address2.length() == 0) {
            System.err.println("Invalid address");
        } else {
            Multiaddr targetAddress = new Multiaddr(address2);
            while (true) {
                System.out.println("Enter msg:");
                byte[] msg = in.nextLine().getBytes();
                FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", Unpooled.copiedBuffer(msg));
                httpRequest.headers().set(HttpHeaderNames.CONTENT_LENGTH, msg.length);
                HttpProtocol.HttpController proxier = node1Http.dial(node, targetAddress).getController().join();
                proxier.send(httpRequest.retain()).join().release();
            }
        }
    }
    public static void main(String[] args) {
        new Chat();
    }
}
