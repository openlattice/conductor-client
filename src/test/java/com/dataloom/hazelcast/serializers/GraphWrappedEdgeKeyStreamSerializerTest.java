package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.UUID;

import com.dataloom.graph.core.mapstores.EdgesMapstore;
import com.dataloom.graph.core.objects.GraphWrappedEdgeKey;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class GraphWrappedEdgeKeyStreamSerializerTest
        extends AbstractStreamSerializerTest<GraphWrappedEdgeKeyStreamSerializer, GraphWrappedEdgeKey>
        implements Serializable {

    private static final long serialVersionUID = -1211589241431379957L;

    @Override
    protected GraphWrappedEdgeKey createInput() {
        return new GraphWrappedEdgeKey( UUID.randomUUID(), EdgesMapstore.generateTestEdgeKey() );
    }

    @Override
    protected GraphWrappedEdgeKeyStreamSerializer createSerializer() {
        return new GraphWrappedEdgeKeyStreamSerializer();
    }

}
