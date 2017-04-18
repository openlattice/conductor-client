package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import com.dataloom.graph.core.mapstores.EdgesMapstore;
import com.dataloom.graph.edge.EdgeKey;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class EdgeKeyStreamSerializerTest extends AbstractStreamSerializerTest<EdgeKeyStreamSerializer, EdgeKey>
        implements Serializable {

    private static final long serialVersionUID = -3800645517483007314L;

    @Override
    protected EdgeKey createInput() {
        return EdgesMapstore.generateTestEdgeKey();
    }

    @Override
    protected EdgeKeyStreamSerializer createSerializer() {
        return new EdgeKeyStreamSerializer();
    }

}
