package com.dataloom.organizations.processors;

import java.util.UUID;

import com.dataloom.authorization.Principal;
import com.dataloom.organizations.PrincipalSet;
import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;

public class PrincipalMerger extends AbstractMerger<UUID, PrincipalSet, Principal> {
    public PrincipalMerger( Iterable<Principal> objects ) {
        super( objects );
    }

    private static final long serialVersionUID = -6923080316858930293L;

    @Override
    protected PrincipalSet newEmptyCollection() {
        return new PrincipalSet( Sets.newHashSet() );
    }

}
