package org.peergos;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import org.junit.Assert;
import org.junit.Test;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.net.APIHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;

public class APIHandlerTest {

    @Test
    public void integrationTest() {
        HttpServer apiServer = null;
        try {
            MultiAddress apiAddress = new MultiAddress("/ip4/127.0.0.1/tcp/5001");
            InetSocketAddress localAPIAddress = new InetSocketAddress(apiAddress.getHost(), apiAddress.getPort());

            apiServer = HttpServer.create(localAPIAddress, 500);
            APIService service = new APIService(new RamBlockstore());
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
