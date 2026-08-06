package org.peergos;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.junit.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

/** An address book that only ever unions in new addresses grows without bound: a peer that changes
 *  address, or spoofs a few hundred, leaves every one of them behind forever. Addresses we have not
 *  seen announced for a long time are stale and should be dropped.
 */
public class AddressBookPruningTest {

    private static final long DAY = 24 * 3600_000L;

    private final AtomicLong now = new AtomicLong(1_000_000L);

    private RamAddressBook book() {
        return new RamAddressBook(DAY, now::get);
    }

    @Test
    public void addressesNotSeenRecentlyArePruned() {
        RamAddressBook book = book();
        PeerId peer = randomPeerId();
        Multiaddr old = new Multiaddr("/ip4/8.1.1.1/tcp/4001");
        book.addAddrs(peer, 0, old).join();

        now.addAndGet(2 * DAY);
        Multiaddr fresh = new Multiaddr("/ip4/8.2.2.2/tcp/4001");
        book.addAddrs(peer, 0, fresh).join();

        Assert.assertEquals(Set.of(fresh), Set.copyOf(book.getAddrs(peer).join()));
    }

    @Test
    public void reannouncingKeepsAnAddressAlive() {
        RamAddressBook book = book();
        PeerId peer = randomPeerId();
        Multiaddr addr = new Multiaddr("/ip4/8.1.1.1/tcp/4001");

        for (int i = 0; i < 5; i++) {
            book.addAddrs(peer, 0, addr).join();
            now.addAndGet(DAY / 2);
        }

        Assert.assertEquals("a peer that keeps announcing should stay reachable",
                Set.of(addr), Set.copyOf(book.getAddrs(peer).join()));
    }

    @Test
    public void addressesPerPeerAreBounded() {
        RamAddressBook book = book();
        PeerId peer = randomPeerId();
        Multiaddr[] many = IntStream.range(0, 500)
                .mapToObj(i -> new Multiaddr("/ip4/8." + (i / 256) + "." + (i % 256) + ".7/tcp/4001"))
                .toArray(Multiaddr[]::new);
        book.addAddrs(peer, 0, many).join();

        int held = book.getAddrs(peer).join().size();
        Assert.assertTrue("held " + held + " addresses for one peer",
                held <= RamAddressBook.MAX_ADDRESSES_PER_PEER);
    }

    @Test
    public void theNewestAddressesAreTheOnesKept() {
        RamAddressBook book = book();
        PeerId peer = randomPeerId();
        for (int i = 0; i < RamAddressBook.MAX_ADDRESSES_PER_PEER; i++) {
            book.addAddrs(peer, 0, new Multiaddr("/ip4/8.0." + i + ".7/tcp/4001")).join();
            now.addAndGet(1000);
        }
        Multiaddr newest = new Multiaddr("/ip4/9.9.9.9/tcp/4001");
        book.addAddrs(peer, 0, newest).join();

        Collection<Multiaddr> held = book.getAddrs(peer).join();
        Assert.assertTrue("the most recently announced address should survive eviction",
                held.contains(newest));
        Assert.assertTrue(held.size() <= RamAddressBook.MAX_ADDRESSES_PER_PEER);
    }

    private static PeerId randomPeerId() {
        byte[] hash = new byte[32];
        new Random().nextBytes(hash);
        return new PeerId(new Multihash(Multihash.Type.sha2_256, hash).toBytes());
    }
}
