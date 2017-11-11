package com.dataloom.hazelcast.serializers;

import com.dataloom.organizations.processors.OrganizationAppRemover;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

import java.util.UUID;

public class OrganizationAppRemoverStreamSerializerTest
        extends AbstractStreamSerializerTest<OrganizationAppRemoverStreamSerializer, OrganizationAppRemover> {
    @Override protected OrganizationAppRemoverStreamSerializer createSerializer() {
        return new OrganizationAppRemoverStreamSerializer();
    }

    @Override protected OrganizationAppRemover createInput() {
        return new OrganizationAppRemover( ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ) );
    }
}
