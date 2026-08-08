package org.peergos;

import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.protocol.*;

import java.util.*;

/** The remote address of an incoming connection is only dialable if the peer dialled us from the port
 *  they listen on. A peer that dials from an ephemeral port, or whose source port is rewritten by a NAT,
 *  gives us an address nothing is listening on. Caching it leaves a "connection refused" entry in the
 *  address book, which we then also hand out to anyone who asks us for that peer.
 */
public class InboundAddressTest {

    @Test
    public void anIncomingConnectionFromAnEphemeralPortContributesNoAddress() throws Exception {
        RamAddressBook dialerBook = new RamAddressBook();
        RamAddressBook listenerBook = new RamAddressBook();
        int listenerPort = TestPorts.getPort();
        // A dialer with no listen addresses has no port to reuse, so it dials from an ephemeral one -
        // the same address the listener sees from any peer behind a port rewriting NAT.
        Host dialer = build(dialerBook, Optional.empty());
        Host listener = build(listenerBook, Optional.of(listenerPort));
        dialer.start().join();
        listener.start().join();
        IdentifyBuilder.addIdentifyProtocol(dialer, Collections.emptyList());
        IdentifyBuilder.addIdentifyProtocol(listener, Collections.emptyList());
        try {
            Multiaddr listenerAddr = Multiaddr.fromString("/ip4/127.0.0.1/tcp/" + listenerPort + "/p2p/" + listener.getPeerId());
            new Ping().dial(dialer, listenerAddr).getController().join().ping().join();
            Thread.sleep(2000); // let identify complete, it runs after the connection handler

            Collection<Multiaddr> heldByListener = listenerBook.getAddrs(dialer.getPeerId()).join();
            Assert.assertEquals("an incoming peer's source port is not an address we can dial",
                    Collections.emptyList(), new ArrayList<>(heldByListener));

            Collection<Multiaddr> heldByDialer = dialerBook.getAddrs(listener.getPeerId()).join();
            Assert.assertTrue("the address we dialled is dialable, so it should be kept: " + heldByDialer,
                    heldByDialer.stream().anyMatch(a -> tcpPort(a).equals(Optional.of(listenerPort))));
        } finally {
            dialer.stop();
            listener.stop();
        }
    }

    private static Optional<Integer> tcpPort(Multiaddr addr) {
        MultiaddrComponent tcp = addr.getFirstComponent(Protocol.TCP);
        return Optional.ofNullable(tcp).map(c -> Integer.parseInt(c.getStringValue()));
    }

    private static Host build(AddressBook book, Optional<Integer> listenPort) {
        return new HostBuilder(book)
                .generateIdentity()
                .listen(listenPort.map(p -> List.of(new io.ipfs.multiaddr.MultiAddress("/ip4/127.0.0.1/tcp/" + p)))
                        .orElse(Collections.emptyList()))
                .addProtocols(List.of(new Ping()))
                .build();
    }
}
