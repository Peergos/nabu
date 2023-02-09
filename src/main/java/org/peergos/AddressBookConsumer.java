package org.peergos;

import io.libp2p.core.*;

public interface AddressBookConsumer {
    void setAddressBook(AddressBook addrs);
}
