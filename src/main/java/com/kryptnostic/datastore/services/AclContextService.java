package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;

public final class AclContextService {
    private AclContextService() {

    }

    public static final Set<UUID> getCurrentContextAclIds() {
        return ImmutableSet.of( ACLs.EVERYONE_ACL );
    }
}
