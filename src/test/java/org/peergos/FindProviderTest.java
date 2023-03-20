package org.peergos;

import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.*;
import io.libp2p.core.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

public class FindProviderTest {

    @Test
    @Ignore // TODO: Figure out why these get provider calls timeout
    public void findBlockProvider() {
        RamBlockstore blockstore = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), blockstore, (c, b, p, a) -> CompletableFuture.completedFuture(true));
        Host node1 = builder1.build();
        node1.start().join();

        try {
            // Don't connect to local kubo
            List<MultiAddress> bootStrapNodes = List.of(
                            "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
                            "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
                            "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
                            "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
                            "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ", // mars.i.ipfs.io
                            "/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
                            "/ip4/104.236.179.241/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
                            "/ip4/128.199.219.111/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
                            "/ip4/104.236.76.40/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
                            "/ip4/178.62.158.247/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
                            "/ip6/2604:a880:1:20:0:0:203:d001/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
                            "/ip6/2400:6180:0:d0:0:0:151:6001/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
                            "/ip6/2604:a880:800:10:0:0:4a:5001/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
                            "/ip6/2a03:b0c0:0:1010:0:0:23:1001/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd"
                    ).stream()
                    .map(MultiAddress::new)
                    .collect(Collectors.toList());
            Kademlia dht = builder1.getWanDht().get();
            Predicate<String> bootstrapAddrFilter = addr -> !addr.contains("/wss/"); // jvm-libp2p can't parse /wss addrs
            int connections = dht.bootstrapRoutingTable(node1, bootStrapNodes, bootstrapAddrFilter);
            if (connections == 0)
                throw new IllegalStateException("No connected peers!");
            dht.bootstrap(node1);

            Multihash block = Cid.decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi");
            List<PeerAddresses> providers = dht.findProviders(block, node1, 10).join();
            if (providers.isEmpty())
                throw new IllegalStateException("Couldn't find provider of block!");
        } finally {
            node1.stop();
        }
    }
}
