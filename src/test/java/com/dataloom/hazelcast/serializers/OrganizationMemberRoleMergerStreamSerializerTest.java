package com.dataloom.hazelcast.serializers;

import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organizations.processors.OrganizationMemberRoleMerger;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class OrganizationMemberRoleMergerStreamSerializerTest extends
        AbstractStreamSerializerTest<OrganizationMemberRoleMergerStreamSerializer, OrganizationMemberRoleMerger> {

    @Override
    protected OrganizationMemberRoleMergerStreamSerializer createSerializer() {
        return new OrganizationMemberRoleMergerStreamSerializer();
    }

    @Override
    protected OrganizationMemberRoleMerger createInput() {
        return new OrganizationMemberRoleMerger( ImmutableSet.of(
                TestDataFactory.userPrincipal(),
                TestDataFactory.userPrincipal()
        ) );
    }
}
