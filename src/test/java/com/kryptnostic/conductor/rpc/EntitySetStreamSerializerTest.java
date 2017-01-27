package com.kryptnostic.conductor.rpc;

import java.io.Serializable;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.mapstores.TestDataFactory;
import com.kryptnostic.conductor.rpc.serializers.EntitySetStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class EntitySetStreamSerializerTest extends AbstractStreamSerializerTest<EntitySetStreamSerializer, EntitySet>
        implements Serializable {
    private static final long serialVersionUID = 8869472746330274551L;

    @Override
    protected EntitySet createInput() {
        return TestDataFactory.entitySet();
    }

    @Override
    protected EntitySetStreamSerializer createSerializer() {
        return new EntitySetStreamSerializer();
    }

}
