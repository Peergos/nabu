package org.peergos;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.api.cbor.CborObject;
import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import org.junit.Assert;
import org.junit.Test;
import org.peergos.blockstore.Blockstore;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.blockstore.TypeLimitedBlockstore;
import org.peergos.client.MerkleNode;
import org.peergos.client.NabuClient;
import org.peergos.net.APIHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executors;

public class APIHandlerTest {

    @Test
    public void codecTest() {
        HttpServer apiServer = null;
        try {
            MultiAddress apiAddress = new MultiAddress("/ip4/127.0.0.1/tcp/8123");
            InetSocketAddress localAPIAddress = new InetSocketAddress(apiAddress.getHost(), apiAddress.getPort());

            apiServer = HttpServer.create(localAPIAddress, 500);
            Blockstore blocks = new TypeLimitedBlockstore(new RamBlockstore(), Set.of(Cid.Codec.Raw));
            APIService service = new APIService(blocks, new BitswapBlockService(null, null));
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
    public void integrationTest() {
        HttpServer apiServer = null;
        try {
            MultiAddress apiAddress = new MultiAddress("/ip4/127.0.0.1/tcp/8123");
            InetSocketAddress localAPIAddress = new InetSocketAddress(apiAddress.getHost(), apiAddress.getPort());

            apiServer = HttpServer.create(localAPIAddress, 500);
            APIService service = new APIService(new RamBlockstore(), new BitswapBlockService(null, null));
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
