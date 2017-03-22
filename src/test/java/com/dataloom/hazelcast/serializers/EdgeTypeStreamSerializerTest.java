package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import com.dataloom.edm.type.EdgeType;
import com.dataloom.mapstores.TestDataFactory;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class EdgeTypeStreamSerializerTest extends AbstractStreamSerializerTest<EdgeTypeStreamSerializer, EdgeType>
        implements Serializable {
    private static final long serialVersionUID = 3237651698696026564L;

    @Override
    protected EdgeType createInput() {
        return TestDataFactory.edgeType();
    }

    @Override
    protected EdgeTypeStreamSerializer createSerializer() {
        return new EdgeTypeStreamSerializer();
    }

}
