package org.peergos.util;

import org.peergos.APIService;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;


public class Admin implements InstanceAdmin {

    private final String sourceVersion;

    public Admin() {
        this.sourceVersion = getSourceVersion();
    }

    @Override
    public CompletableFuture<VersionInfo> getVersionInfo() {
        return CompletableFuture.completedFuture(new VersionInfo(APIService.CURRENT_VERSION, sourceVersion));
    }

    public static String getSourceVersion() {
        return Optional.ofNullable(Admin.class.getPackage().getImplementationVersion()).orElse("");
    }

}
