package org.peergos;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.api.cbor.CborObject;
import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.Host;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.peergos.blockstore.Blockstore;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.blockstore.TypeLimitedBlockstore;
import org.peergos.client.MerkleNode;
import org.peergos.client.NabuClient;
import org.peergos.net.APIHandler;
import org.peergos.protocol.dht.Kademlia;
import org.peergos.protocol.dht.RamProviderStore;
import org.peergos.protocol.dht.RamRecordStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class APIHandlerTest {

    @Test
    public void codecTest() {
        HttpServer apiServer = null;
        try {
            MultiAddress apiAddress = new MultiAddress("/ip4/127.0.0.1/tcp/8456");
            InetSocketAddress localAPIAddress = new InetSocketAddress(apiAddress.getHost(), apiAddress.getPort());

            apiServer = HttpServer.create(localAPIAddress, 500);
            Blockstore blocks = new TypeLimitedBlockstore(new RamBlockstore(), Set.of(Cid.Codec.Raw));
            APIService service = new APIService(blocks, new BitswapBlockService(null, null), null);
            apiServer.createContext(APIService.API_URL, new APIHandler(service, null));
            apiServer.setExecutor(Executors.newFixedThreadPool(50));
            apiServer.start();

            NabuClient nabu = new NabuClient(apiAddress.getHost(), apiAddress.getPort(), "/api/v0/", false);
            String version = nabu.version();
            Assert.assertTrue("version", version != null);
            // node not initialised Map id = ipfs.id();
            String text = "Hello world!";
            byte[] block = text.getBytes();
            MerkleNode added = nabu.putBlock(block, Optional.of("raw"));
            try {
                //should fail as dag-cbor not in list of accepted codecs
                Map<String, CborObject> tmp = new LinkedHashMap<>();
                tmp.put("data", new CborObject.CborString("testing"));
                CborObject original = CborObject.CborMap.build(tmp);
                byte[] object = original.toByteArray();
                MerkleNode added2 = nabu.putBlock(object, Optional.of("dag-cbor"));
                Assert.assertTrue("codec accepted", false);
            } catch (Exception e) {
                //expected
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
            Assert.assertTrue("IOException", false);
        } finally {
            if (apiServer != null) {
                apiServer.stop(1);
            }
        }
    }
    @Test
    @Ignore
    public void findBlockProviderTest() throws IOException {
        RamBlockstore blockstore = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), blockstore, (c, b, p, a) -> CompletableFuture.completedFuture(true));
        Host node1 = builder1.build();
        node1.start().join();
        HttpServer apiServer = null;
        try {
            Kademlia dht = builder1.getWanDht().get();
            Predicate<String> bootstrapAddrFilter = addr -> !addr.contains("/wss/"); // jvm-libp2p can't parse /wss addrs
            int connections = dht.bootstrapRoutingTable(node1, BootstrapTest.BOOTSTRAP_NODES, bootstrapAddrFilter);
            if (connections == 0)
                throw new IllegalStateException("No connected peers!");
            dht.bootstrap(node1);

            MultiAddress apiAddress = new MultiAddress("/ip4/127.0.0.1/tcp/8123");
            InetSocketAddress localAPIAddress = new InetSocketAddress(apiAddress.getHost(), apiAddress.getPort());

            apiServer = HttpServer.create(localAPIAddress, 500);
            APIService service = new APIService(new RamBlockstore(), new BitswapBlockService(null, null), dht);
            apiServer.createContext(APIService.API_URL, new APIHandler(service, node1));
            apiServer.setExecutor(Executors.newFixedThreadPool(50));
            apiServer.start();

            IPFS ipfs = new IPFS(apiAddress.getHost(), apiAddress.getPort(), "/api/v0/", false, false);
            Cid cid = Cid.decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi");
            List<Map<String, Object>> providers = ipfs.dht.findprovs(cid);
            if (providers.isEmpty())
                throw new IllegalStateException("Couldn't find provider of block!");
        } finally {
            node1.stop();
            if (apiServer != null) {
                apiServer.stop(1);
            }
        }
    }
    @Test
    public void blockMethodsTest() {
        HttpServer apiServer = null;
        try {
            MultiAddress apiAddress = new MultiAddress("/ip4/127.0.0.1/tcp/8123");
            InetSocketAddress localAPIAddress = new InetSocketAddress(apiAddress.getHost(), apiAddress.getPort());

            apiServer = HttpServer.create(localAPIAddress, 500);
            APIService service = new APIService(new RamBlockstore(), new BitswapBlockService(null, null), null);
            apiServer.createContext(APIService.API_URL, new APIHandler(service, null));
            apiServer.setExecutor(Executors.newFixedThreadPool(50));
            apiServer.start();

            NabuClient nabu = new NabuClient(apiAddress.getHost(), apiAddress.getPort(), "/api/v0/", false);
            String version = nabu.version();
            Assert.assertTrue("version", version != null);
            // node not initialised Map id = ipfs.id();
            String text = "Hello world!";
            byte[] block = text.getBytes();
            MerkleNode added = nabu.putBlock(block, Optional.of("raw"));

            Map stat = nabu.stat(added.hash);
            int size = (Integer)stat.get("Size");
            Assert.assertTrue("size as expected", size == text.length());

            boolean has = nabu.hasBlock(added.hash, Optional.empty());
            Assert.assertTrue("has block as expected", has);

            boolean bloomAdd = nabu.bloomAdd(added.hash);
            Assert.assertTrue("added to bloom filter", !bloomAdd); //RamBlockstore does not filter

            byte[] data = nabu.getBlock(added.hash, Optional.empty());
            Assert.assertTrue("block is as expected", text.equals(new String(data)));

            List<Multihash> localRefs = nabu.listBlockstore();
            Assert.assertTrue("local ref size", localRefs.size() == 1);

            nabu.removeBlock(added.hash);
            List<Multihash> localRefsAfter = nabu.listBlockstore();
            Assert.assertTrue("local ref size after rm", localRefsAfter.size() == 0);

            boolean have = nabu.hasBlock(added.hash, Optional.empty());
            Assert.assertTrue("does not have block as expected", !have);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            Assert.assertTrue("IOException", false);
        } finally {
            if (apiServer != null) {
                apiServer.stop(1);
            }
        }
    }
}
