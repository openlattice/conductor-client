package com.dataloom.organizations.processors;

import com.dataloom.authorization.Principal;
import com.dataloom.organizations.PrincipalSet;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;
import java.util.HashSet;

public class NestedPrincipalMerger extends AbstractMerger<Principal, PrincipalSet, Principal> {

    private static final long serialVersionUID = 4127837036304775975L;

    public NestedPrincipalMerger( Iterable<Principal> principalsToAdd ) {
        super( principalsToAdd );
    }

    @Override
    protected PrincipalSet newEmptyCollection() {
        return new PrincipalSet( new HashSet<>() );
    }
}
