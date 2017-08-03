package com.dataloom.organizations.processors;

import com.dataloom.authorization.Principal;
import com.dataloom.organizations.PrincipalSet;
import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;

import java.util.UUID;

public class OrganizationMemberRoleMerger extends AbstractMerger<UUID, PrincipalSet, Principal> {

    public OrganizationMemberRoleMerger( Iterable<Principal> objects ) {
        super( objects );
    }

    @Override
    protected PrincipalSet newEmptyCollection() {
        return new PrincipalSet( Sets.newHashSet() );
    }
}
