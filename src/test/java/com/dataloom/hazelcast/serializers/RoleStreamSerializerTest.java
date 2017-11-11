package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.UUID;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.mapstores.TestDataFactory;
import com.openlattice.authorization.SecurablePrincipal;
import org.apache.commons.lang3.RandomStringUtils;

import com.dataloom.organization.roles.Role;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class RoleStreamSerializerTest
        extends AbstractStreamSerializerTest<RoleStreamSerializer, SecurablePrincipal>
        implements Serializable {

    private static final long serialVersionUID = 8223378929816938716L;

    @Override
    public Role createInput() {
        return TestDataFactory.role();
    }

    @Override
    public RoleStreamSerializer createSerializer() {
        return new RoleStreamSerializer();
    }

}
