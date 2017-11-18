package com.dataloom.hazelcast.serializers;

import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organization.roles.Role;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

import java.io.Serializable;

public class RoleStreamSerializerTest
        extends AbstractStreamSerializerTest<RoleStreamSerializer, Role>
        implements Serializable {

    private static final long serialVersionUID = 8223378929816938716L;

    @Override
    protected Role createInput() {
        return TestDataFactory.role();
    }

    @Override
    public RoleStreamSerializer createSerializer() {
        return new RoleStreamSerializer();
    }

}
