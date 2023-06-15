package org.peergos.config;

import io.ipfs.multiaddr.MultiAddress;
import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.PrivKey;
import org.peergos.HostBuilder;
import org.peergos.util.JSONParser;
import org.peergos.util.JsonHelper;

import java.util.*;
import java.util.stream.Collectors;

public class Config {

    public final AddressesSection addresses;
    public final BootstrapSection bootstrap;
    public final DatastoreSection datastore;
    public final IdentitySection identity;

    public Config() {
        Config config = defaultConfig();
        this.addresses = config.addresses;
        this.bootstrap = config.bootstrap;
        this.datastore = config.datastore;
        this.identity = config.identity;
    }

    public Config(AddressesSection addresses, BootstrapSection bootstrap, DatastoreSection datastore, IdentitySection identity) {
        this.addresses = addresses;
        this.bootstrap = bootstrap;
        this.datastore = datastore;
        this.identity = identity;
        validate(this);
    }

    public static Config build(String contents) {
        Map<String, Object> json = (Map) JSONParser.parse(contents);
        AddressesSection addressesSection = Jsonable.parse(json, p -> AddressesSection.fromJson(p));
        BootstrapSection bootstrapSection = Jsonable.parse(json, p -> BootstrapSection.fromJson(p));
        DatastoreSection datastoreSection = Jsonable.parse(json, p -> DatastoreSection.fromJson(p));
        IdentitySection identitySection = Jsonable.parse(json, p -> IdentitySection.fromJson(p));
        return new Config(addressesSection, bootstrapSection, datastoreSection, identitySection);
    }

    @Override
    public String toString() {
        Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.putAll(addresses.toJson());
        configMap.putAll(bootstrap.toJson());
        configMap.putAll(datastore.toJson());
        configMap.putAll(identity.toJson());
        return JsonHelper.pretty(configMap);
    }

    public Config defaultConfig() {
        HostBuilder builder = new HostBuilder().generateIdentity();
        PrivKey privKey = builder.getPrivateKey();
        PeerId peerId = builder.getPeerId();

        List<MultiAddress> swarmAddresses = List.of(new MultiAddress("/ip6/::/tcp/4001"));
        MultiAddress apiAddress = new MultiAddress("/ip4/127.0.0.1/tcp/5001");
        MultiAddress gatewayAddress = new MultiAddress("/ip4/127.0.0.1/tcp/8080");
        Optional<MultiAddress> proxyTargetAddress = Optional.of(new MultiAddress("/ip4/127.0.0.1/tcp/8000"));

        Optional<String> allowTarget = Optional.of("http://localhost:8002");
        List<MultiAddress> bootstrapNodes = List.of(
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
                        "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ", // mars.i.ipfs.io
                        "/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ").stream()
                .map(MultiAddress::new)
                .collect(Collectors.toList());

        Map<String, Object> blockChildMap = new LinkedHashMap<>();
        blockChildMap.put("path", "blocks");
        blockChildMap.put("shardFunc", "/repo/flatfs/shard/v1/next-to-last/2");
        blockChildMap.put("sync", "true");
        blockChildMap.put("type", "flatfs");
        Mount blockMount = new Mount("/blocks", "flatfs.datastore", "measure", blockChildMap);

        Map<String, Object> dataChildMap = new LinkedHashMap<>();
        dataChildMap.put("compression", "none");
        dataChildMap.put("path", "datastore");
        dataChildMap.put("type", "h2");
        Mount rootMount = new Mount("/", "h2.datastore", "measure", dataChildMap);

        AddressesSection addressesSection = new AddressesSection(swarmAddresses, apiAddress, gatewayAddress,
                proxyTargetAddress, allowTarget);
        Filter filter = new Filter(FilterType.NONE, 0.0);
        CodecSet codecSet = CodecSet.empty();
        DatastoreSection datastoreSection = new DatastoreSection(blockMount, rootMount, filter, codecSet);
        BootstrapSection bootstrapSection = new BootstrapSection(bootstrapNodes);
        IdentitySection identity = new IdentitySection(privKey.bytes(), peerId);
        return new Config(addressesSection, bootstrapSection, datastoreSection, identity);
    }

    public void validate(Config config) {

        if (config.addresses.getSwarmAddresses().isEmpty()) {
            throw new IllegalStateException("Expecting Addresses/Swarm entries");
        }
        Mount blockMount = config.datastore.blockMount;
        if (!(blockMount.prefix.equals("flatfs.datastore") && blockMount.type.equals("measure"))) {
            throw new IllegalStateException("Expecting /blocks mount to have prefix == 'flatfs.datastore' and type == 'measure'");
        }
        Map<String, Object> blockParams = blockMount.getParams();
        String blockPath = (String) blockParams.get("path");
        String blockShardFunc = (String) blockParams.get("shardFunc");
        String blockType = (String) blockParams.get("type");
        if (!(blockPath.equals("blocks") && blockShardFunc.equals("/repo/flatfs/shard/v1/next-to-last/2")
                && blockType.equals("flatfs"))) {
            throw new IllegalStateException("Expecting flatfs mount at /blocks");
        }

        Mount rootMount = config.datastore.rootMount;
        if (!(rootMount.prefix.equals("h2.datastore") && rootMount.type.equals("measure"))) {
            throw new IllegalStateException("Expecting / mount to have prefix == 'h2.datastore' and type == 'measure'");
        }
        Map<String, Object> rootParams = rootMount.getParams();
        String rootPath = (String) rootParams.get("path");
        String rootCompression = (String) rootParams.get("compression");
        String rootType = (String) rootParams.get("type");
        if (!(rootPath.equals("datastore") && rootCompression.equals("none") && rootType.equals("h2"))) {
            throw new IllegalStateException("Expecting flatfs mount at /blocks");
        }
    }
}
