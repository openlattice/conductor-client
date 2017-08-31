package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.UUID;

import com.dataloom.edm.set.EntitySetPropertyKey;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class EntitySetPropertyKeyStreamSerializerTest
        extends AbstractStreamSerializerTest<EntitySetPropertyKeyStreamSerializer, EntitySetPropertyKey>
        implements Serializable {
    private static final long serialVersionUID = -4933403371497497344L;

    @Override
    protected EntitySetPropertyKeyStreamSerializer createSerializer() {
        return new EntitySetPropertyKeyStreamSerializer();
    }

    @Override
    protected EntitySetPropertyKey createInput() {
        return new EntitySetPropertyKey( UUID.randomUUID(), UUID.randomUUID() );
    }

}
