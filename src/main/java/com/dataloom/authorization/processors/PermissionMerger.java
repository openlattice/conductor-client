package com.dataloom.authorization.processors;

import java.util.HashSet;
import java.util.Set;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.requests.Permission;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;

public class PermissionMerger extends AbstractMerger<AceKey, Set<Permission>, Permission >{
    private static final long serialVersionUID = -3504613417625318717L;
    
    public PermissionMerger( Iterable<Permission> objects ) {
        super( objects );
    }

    @Override
    protected Set<Permission> newEmptyCollection() {
        return new HashSet<>();
    }
}
