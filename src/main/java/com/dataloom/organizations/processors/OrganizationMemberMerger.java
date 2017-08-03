package com.dataloom.organizations.processors;

import com.dataloom.authorization.Principal;
import com.dataloom.organizations.PrincipalSet;
import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;

import java.util.UUID;

public class OrganizationMemberMerger extends AbstractMerger<UUID, PrincipalSet, Principal> {

    private static final long serialVersionUID = 5404127334102098647L;

    public OrganizationMemberMerger( Iterable<Principal> objects ) {
        super( objects );
    }

    @Override
    protected PrincipalSet newEmptyCollection() {
        return new PrincipalSet( Sets.newHashSet() );
    }

}
