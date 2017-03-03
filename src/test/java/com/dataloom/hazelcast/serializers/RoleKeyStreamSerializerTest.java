package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.UUID;

import com.dataloom.organizations.roles.RoleKey;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class RoleKeyStreamSerializerTest extends AbstractStreamSerializerTest<RoleKeyStreamSerializer, RoleKey>
        implements Serializable {

    private static final long serialVersionUID = -5255492667631073127L;

    @Override
    protected RoleKey createInput() {
        return new RoleKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override
    protected RoleKeyStreamSerializer createSerializer() {
        return new RoleKeyStreamSerializer();
    }

}
