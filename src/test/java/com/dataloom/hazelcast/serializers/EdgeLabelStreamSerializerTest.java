package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import com.dataloom.graph.core.mapstores.EdgesMapstore;
import com.dataloom.graph.core.objects.EdgeLabel;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class EdgeLabelStreamSerializerTest extends AbstractStreamSerializerTest<EdgeLabelStreamSerializer, EdgeLabel>
        implements Serializable {

    private static final long serialVersionUID = -3432556526759007006L;

    @Override
    protected EdgeLabel createInput() {
        return EdgesMapstore.generateTestEdgeLabel();
    }

    @Override
    protected EdgeLabelStreamSerializer createSerializer() {
        return new EdgeLabelStreamSerializer();
    }

}
