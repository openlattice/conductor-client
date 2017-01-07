package com.dataloom.authorization.processors;

import java.util.Set;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.requests.Permission;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;

public class PermissionRemover extends AbstractRemover<AceKey, Set<Permission>, Permission> {
    private static final long serialVersionUID = 541402002243327088L;

    public PermissionRemover( Iterable<Permission> objectsToRemove ) {
        super( objectsToRemove );
    }
}
