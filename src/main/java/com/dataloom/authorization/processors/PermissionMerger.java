package com.dataloom.authorization.processors;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.Permission;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;

public class PermissionMerger extends AbstractMerger<AceKey, DelegatedPermissionEnumSet, Permission> {
    private static final long serialVersionUID = -3504613417625318717L;

    public PermissionMerger( Iterable<Permission> objects ) {
        super( objects );
    }

    @Override
    protected DelegatedPermissionEnumSet newEmptyCollection() {
        return new DelegatedPermissionEnumSet();
    }
}
