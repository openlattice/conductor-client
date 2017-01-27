package com.kryptnostic.conductor.rpc;

import java.io.Serializable;

import com.dataloom.edm.internal.PropertyType;
import com.dataloom.mapstores.TestDataFactory;
import com.kryptnostic.conductor.rpc.serializers.PropertyTypeStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class PropertyTypeStreamSerializerTest extends AbstractStreamSerializerTest<PropertyTypeStreamSerializer, PropertyType>
        implements Serializable {
    private static final long serialVersionUID = 8869472746330274551L;

    @Override
    protected PropertyType createInput() {
        return TestDataFactory.propertyType();
    }

    @Override
    protected PropertyTypeStreamSerializer createSerializer() {
        return new PropertyTypeStreamSerializer();
    }

}
