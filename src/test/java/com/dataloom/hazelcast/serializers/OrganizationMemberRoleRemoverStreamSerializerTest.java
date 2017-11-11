package com.dataloom.hazelcast.serializers;

import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organizations.processors.NestedPrincipalRemover;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class OrganizationMemberRoleRemoverStreamSerializerTest extends
        AbstractStreamSerializerTest<OrganizationMemberRoleRemoverStreamSerializer, NestedPrincipalRemover> {

    @Override
    protected OrganizationMemberRoleRemoverStreamSerializer createSerializer() {
        return new OrganizationMemberRoleRemoverStreamSerializer();
    }

    @Override
    protected NestedPrincipalRemover createInput() {
        return new NestedPrincipalRemover( ImmutableSet.of(
                TestDataFactory.userPrincipal(),
                TestDataFactory.userPrincipal()
        ) );
    }
}
