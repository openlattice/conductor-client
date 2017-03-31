package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.UUID;

import com.dataloom.graph.core.objects.GraphWrappedVertexId;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class GraphWrappedVertexIdStreamSerializerTest
        extends AbstractStreamSerializerTest<GraphWrappedVertexIdStreamSerializer, GraphWrappedVertexId>
        implements Serializable {

    private static final long serialVersionUID = 2597679703069616006L;

    @Override
    protected GraphWrappedVertexId createInput() {
        return new GraphWrappedVertexId( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override
    protected GraphWrappedVertexIdStreamSerializer createSerializer() {
        return new GraphWrappedVertexIdStreamSerializer();
    }

}
