package com.dataloom.hazelcast.serializers;

import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organizations.processors.OrganizationMemberMerger;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class OrganizationMemberMergerStreamSerializerTest extends
        AbstractStreamSerializerTest<OrganizationMemberMergerStreamSerializer, OrganizationMemberMerger> {

    @Override
    protected OrganizationMemberMergerStreamSerializer createSerializer() {
        return new OrganizationMemberMergerStreamSerializer();
    }

    @Override
    protected OrganizationMemberMerger createInput() {
        return new OrganizationMemberMerger( ImmutableSet.of(
                TestDataFactory.userPrincipal(),
                TestDataFactory.userPrincipal()
        ) );
    }
}
