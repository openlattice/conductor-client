package com.dataloom.hazelcast.serializers;

import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.organization.Organization;
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
