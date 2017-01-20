package com.dataloom.authorization.processors;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.Permission;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;

public class PermissionRemover extends AbstractRemover<AceKey, DelegatedPermissionEnumSet, Permission> {
    private static final long serialVersionUID = 541402002243327088L;

    public PermissionRemover( Iterable<Permission> objectsToRemove ) {
        super( objectsToRemove );
    }
}
