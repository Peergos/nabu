package org.peergos;

import io.ipfs.multiaddr.*;
import org.junit.*;
import org.peergos.config.*;

import java.util.*;

public class AddressesSectionTest {

    private static AddressesSection section(boolean upnp) {
        return new AddressesSection(
                List.of(new MultiAddress("/ip4/0.0.0.0/tcp/4001")),
                new MultiAddress("/ip4/127.0.0.1/tcp/5001"),
                new MultiAddress("/ip4/127.0.0.1/tcp/8080"),
                Optional.empty(),
                Optional.empty(),
                upnp);
    }

    @Test
    public void upnpFlagRoundTrips() {
        AddressesSection restored = AddressesSection.fromJson(section(true).toJson());
        Assert.assertTrue("UPnP should survive a toJson/fromJson round trip", restored.enableUPnP);
    }

    @Test
    public void upnpDefaultsOffAndIsOmittedWhenDisabled() {
        Assert.assertFalse(section(false).toJson().toString().contains("UPnP"));
        AddressesSection restored = AddressesSection.fromJson(section(false).toJson());
        Assert.assertFalse(restored.enableUPnP);
    }
}
