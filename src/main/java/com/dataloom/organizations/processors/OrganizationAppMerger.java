package com.dataloom.organizations.processors;

import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.objects.UUIDSet;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;

import java.util.UUID;

public class OrganizationAppMerger extends AbstractMerger<UUID, UUIDSet, UUID> {
    private static final long serialVersionUID = 5640080326387143549L;

    public OrganizationAppMerger( Iterable<UUID> objects ) {
        super( objects );
    }

    @Override protected UUIDSet newEmptyCollection() {
        return new UUIDSet( ImmutableSet.of() );
    }
}
