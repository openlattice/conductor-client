package com.kryptnostic.conductor.rpc;

import java.io.Serializable;

import com.dataloom.edm.internal.EntityType;
import com.dataloom.mapstores.TestDataFactory;
import com.kryptnostic.conductor.rpc.serializers.EntityTypeStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.BaseSerializerTest;

public class EntityTypeStreamSerializerTest extends BaseSerializerTest<EntityTypeStreamSerializer, EntityType>
        implements Serializable {
    private static final long serialVersionUID = 8869472746330274551L;

    @Override
    protected EntityType createInput() {
        return TestDataFactory.entityType();
    }

    @Override
    protected EntityTypeStreamSerializer createSerializer() {
        return new EntityTypeStreamSerializer();
    }

}
