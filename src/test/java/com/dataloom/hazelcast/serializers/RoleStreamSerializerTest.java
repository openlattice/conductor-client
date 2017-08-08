package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;

import com.dataloom.organization.roles.Role;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class RoleStreamSerializerTest
        extends AbstractStreamSerializerTest<RoleStreamSerializer, Role>
        implements Serializable {

    private static final long serialVersionUID = 8223378929816938716L;

    @Override
    protected Role createInput() {
        return new Role(
                Optional.of( UUID.randomUUID() ),
                UUID.randomUUID(),
                RandomStringUtils.randomAlphanumeric( 5 ),
                Optional.of( RandomStringUtils.randomAlphanumeric( 5 ) ) );
    }

    @Override
    protected RoleStreamSerializer createSerializer() {
        return new RoleStreamSerializer();
    }

}
