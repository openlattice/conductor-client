package com.dataloom.hazelcast.serializers;

import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organizations.processors.NestedPrincipalMerger;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class OrganizationMemberRoleMergerStreamSerializerTest extends
        AbstractStreamSerializerTest<OrganizationMemberRoleMergerStreamSerializer, NestedPrincipalMerger> {

    @Override
    protected OrganizationMemberRoleMergerStreamSerializer createSerializer() {
        return new OrganizationMemberRoleMergerStreamSerializer();
    }

    @Override
    protected NestedPrincipalMerger createInput() {
        return new NestedPrincipalMerger( ImmutableSet.of(
                TestDataFactory.userPrincipal(),
                TestDataFactory.userPrincipal()
        ) );
    }
}
