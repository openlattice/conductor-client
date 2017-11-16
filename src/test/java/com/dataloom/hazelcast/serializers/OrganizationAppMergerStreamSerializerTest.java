package com.dataloom.hazelcast.serializers;

import com.dataloom.organizations.processors.OrganizationAppMerger;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;

import java.util.UUID;

public class OrganizationAppMergerStreamSerializerTest
        extends AbstractStreamSerializerTest<OrganizationAppMergerStreamSerializer, OrganizationAppMerger> {
    @Override protected OrganizationAppMergerStreamSerializer createSerializer() {
        return new OrganizationAppMergerStreamSerializer();
    }

    @Override protected OrganizationAppMerger createInput() {
        return new OrganizationAppMerger( DelegatedUUIDSet.wrap( ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ) ) );
    }
}
