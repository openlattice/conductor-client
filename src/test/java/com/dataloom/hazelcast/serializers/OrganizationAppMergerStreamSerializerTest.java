package com.dataloom.hazelcast.serializers;

import com.dataloom.organizations.processors.OrganizationAppMerger;
import com.google.common.collect.ImmutableList;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

import java.util.UUID;

public class OrganizationAppMergerStreamSerializerTest
        extends AbstractStreamSerializerTest<OrganizationAppMergerStreamSerializer, OrganizationAppMerger> {
    @Override protected OrganizationAppMergerStreamSerializer createSerializer() {
        return new OrganizationAppMergerStreamSerializer();
    }

    @Override protected OrganizationAppMerger createInput() {
        return new OrganizationAppMerger( ImmutableList.of( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ) );
    }
}
