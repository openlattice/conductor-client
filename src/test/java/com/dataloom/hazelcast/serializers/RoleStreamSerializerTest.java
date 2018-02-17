package com.dataloom.hazelcast.serializers;

import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.organization.roles.Role;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

import com.openlattice.authorization.SecurablePrincipal;
import java.io.Serializable;

public class RoleStreamSerializerTest
        extends AbstractStreamSerializerTest<SecurablePrincipalStreamSerializer, SecurablePrincipal>
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
