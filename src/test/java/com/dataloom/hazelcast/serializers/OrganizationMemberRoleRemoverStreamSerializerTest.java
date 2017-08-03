package com.dataloom.hazelcast.serializers;

import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organizations.processors.OrganizationMemberRoleRemover;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class OrganizationMemberRoleRemoverStreamSerializerTest extends
        AbstractStreamSerializerTest<OrganizationMemberRoleRemoverStreamSerializer, OrganizationMemberRoleRemover> {

    @Override
    protected OrganizationMemberRoleRemoverStreamSerializer createSerializer() {
        return new OrganizationMemberRoleRemoverStreamSerializer();
    }

    @Override
    protected OrganizationMemberRoleRemover createInput() {
        return new OrganizationMemberRoleRemover( ImmutableSet.of(
                TestDataFactory.userPrincipal(),
                TestDataFactory.userPrincipal()
        ) );
    }
}
