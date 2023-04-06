package org.peergos;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.api.cbor.CborObject;
import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.PeerId;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.peergos.blockstore.Blockstore;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.blockstore.TypeLimitedBlockstore;
import org.peergos.client.NabuClient;
import org.peergos.net.APIHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executors;

public class HandlerTest {

    @Test
    @Ignore //requires real Server to be running
    public void serverTest() throws IOException {
        MultiAddress address = new MultiAddress("/ip4/127.0.0.1/tcp/5001");
        NabuClient nabu = new NabuClient(address.getHost(), address.getPort(), "/api/v0/", false);
        String ver = nabu.version();
        PeerId id = nabu.id();
    }

    @Test
    public void codecTest() {
        HttpServer apiServer = null;
        try {
            MultiAddress apiAddress = new MultiAddress("/ip4/127.0.0.1/tcp/8456");
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
            Cid added = nabu.putBlocks(block, Optional.of("raw"));
            try {
                //should fail as dag-cbor not in list of accepted codecs
                Map<String, CborObject> tmp = new LinkedHashMap<>();
                tmp.put("data", new CborObject.CborString("testing"));
                CborObject original = CborObject.CborMap.build(tmp);
                byte[] object = original.toByteArray();
                Cid added2 = nabu.putBlocks(object, Optional.of("dag-cbor"));
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
            Cid addedHash = nabu.putBlocks(block, Optional.of("raw"));

            int size  = nabu.stat(addedHash);
            Assert.assertTrue("size as expected", size == text.length());

            boolean has = nabu.hasBlock(addedHash, Optional.empty());
            Assert.assertTrue("has block as expected", has);

            boolean bloomAdd = nabu.bloomAdd(addedHash);
            Assert.assertTrue("added to bloom filter", !bloomAdd); //RamBlockstore does not filter

            byte[] data = nabu.getBlock(addedHash, Optional.empty());
            Assert.assertTrue("block is as expected", text.equals(new String(data)));

            List<Cid> localRefs = nabu.listBlockstore();
            Assert.assertTrue("local ref size", localRefs.size() == 1);

            nabu.removeBlock(addedHash);
            List<Cid> localRefsAfter = nabu.listBlockstore();
            Assert.assertTrue("local ref size after rm", localRefsAfter.size() == 0);

            boolean have = nabu.hasBlock(addedHash, Optional.empty());
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
