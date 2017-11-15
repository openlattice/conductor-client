package com.dataloom.hazelcast.serializers;

import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organization.Organization;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class OrganizationStreamSerializerTest extends
        AbstractStreamSerializerTest<OrganizationStreamSerializer, Organization> {
    @Override protected OrganizationStreamSerializer createSerializer() {
        return new OrganizationStreamSerializer();
    }

    @Override protected Organization createInput() {
        return TestDataFactory.organization();
    }
}
