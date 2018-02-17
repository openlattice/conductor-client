package com.dataloom.hazelcast.serializers;

import com.openlattice.mapstores.TestDataFactory;
import com.dataloom.organizations.processors.OrganizationMemberRemover;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class OrganizationMemberRemoverStreamSerializerTest extends
        AbstractStreamSerializerTest<OrganizationMemberRemoverStreamSerializer, OrganizationMemberRemover> {

    @Override
    protected OrganizationMemberRemoverStreamSerializer createSerializer() {
        return new OrganizationMemberRemoverStreamSerializer();
    }

    @Override
    protected OrganizationMemberRemover createInput() {
        return new OrganizationMemberRemover( ImmutableSet.of(
                TestDataFactory.userPrincipal(),
                TestDataFactory.userPrincipal()
        ) );
    }
}
