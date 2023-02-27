package org.peergos.util;

import org.peergos.cbor.CborObject;
import org.peergos.cbor.Cborable;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;


public interface InstanceAdmin {

    CompletableFuture<VersionInfo> getVersionInfo();

    class VersionInfo implements Cborable {
        public final Version version;
        public final String sourceVersion;

        public VersionInfo(Version version, String sourceVersion) {
            this.version = version;
            this.sourceVersion = sourceVersion;
        }

        @Override
        public CborObject toCbor() {
            Map<String, Cborable> props = new TreeMap<>();
            props.put("v", new CborObject.CborString(version.toString()));
            props.put("s", new CborObject.CborString(sourceVersion));
            return CborObject.CborMap.build(props);
        }

        public static VersionInfo fromCbor(Cborable cbor) {
            CborObject.CborMap map = (CborObject.CborMap) cbor;
            String version = map.getString("v");
            String sourceVersion = map.getString("s");
            return new VersionInfo(Version.parse(version), sourceVersion);
        }

        @Override
        public String toString() {
            return version + "-" + sourceVersion;
        }
    }
}