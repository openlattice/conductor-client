package com.dataloom.organizations.processors;

import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;

import java.util.UUID;

public class OrganizationAppMerger extends AbstractMerger<UUID, DelegatedUUIDSet, UUID> {
    private static final long serialVersionUID = 5640080326387143549L;

    public OrganizationAppMerger( Iterable<UUID> objects ) {
        super( objects );
    }

    @Override protected DelegatedUUIDSet newEmptyCollection() {
        return new DelegatedUUIDSet( ImmutableSet.of() );
    }
}
