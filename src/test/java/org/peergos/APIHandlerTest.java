package org.peergos;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.api.cbor.CborObject;
import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import org.junit.Assert;
import org.junit.Test;
import org.peergos.blockstore.Blockstore;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.blockstore.TypeLimitedBlockstore;
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

            IPFS ipfs = new IPFS(apiAddress.getHost(), apiAddress.getPort(), "/api/v0/", false, false);
            String version = ipfs.version();
            Assert.assertTrue("version", version != null);
            // node not initialised Map id = ipfs.id();
            String text = "Hello world!";
            byte[] block = text.getBytes();
            MerkleNode added = ipfs.block.put(block, Optional.of("raw"));
            try {
                //should fail as dag-cbor not in list of accepted codecs
                Map<String, CborObject> tmp = new LinkedHashMap<>();
                tmp.put("data", new CborObject.CborString("testing"));
                CborObject original = CborObject.CborMap.build(tmp);
                byte[] object = original.toByteArray();
                MerkleNode added2 = ipfs.block.put(object, Optional.of(Cid.Codec.DagCbor.toString()));
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

            IPFS ipfs = new IPFS(apiAddress.getHost(), apiAddress.getPort(), "/api/v0/", false, false);
            String version = ipfs.version();
            Assert.assertTrue("version", version != null);
            // node not initialised Map id = ipfs.id();
            String text = "Hello world!";
            byte[] block = text.getBytes();
            MerkleNode added = ipfs.block.put(block, Optional.of("raw"));

            Map stat = ipfs.block.stat(added.hash);
            int size = (Integer)stat.get("Size");
            Assert.assertTrue("size as expected", size == text.length());

            byte[] data = ipfs.block.get(added.hash);
            Assert.assertTrue("block is as expected", text.equals(new String(data)));

            List<Multihash> localRefs = ipfs.refs.local();
            Assert.assertTrue("local ref size", localRefs.size() == 1);

            byte[] removed = ipfs.block.rm(added.hash);
            List<Multihash> localRefsAfter = ipfs.refs.local();
            Assert.assertTrue("local ref size after rm", localRefsAfter.size() == 0);
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
