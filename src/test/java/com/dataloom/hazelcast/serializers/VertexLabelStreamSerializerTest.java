package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import com.dataloom.graph.core.objects.VertexLabel;
import com.dataloom.mapstores.TestDataFactory;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class VertexLabelStreamSerializerTest
        extends AbstractStreamSerializerTest<VertexLabelStreamSerializer, VertexLabel>
        implements Serializable {

    private static final long serialVersionUID = 1840631402165130499L;

    @Override
    protected VertexLabel createInput() {
        return new VertexLabel( TestDataFactory.entityKey() );
    }

    @Override
    protected VertexLabelStreamSerializer createSerializer() {
        return new VertexLabelStreamSerializer();
    }

}
