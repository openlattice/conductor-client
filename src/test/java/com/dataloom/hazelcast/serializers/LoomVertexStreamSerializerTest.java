package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.UUID;

import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.mapstores.TestDataFactory;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class LoomVertexStreamSerializerTest extends AbstractStreamSerializerTest<LoomVertexStreamSerializer, LoomVertex>
        implements Serializable {

    private static final long serialVersionUID = -7580519749925921121L;

    @Override
    protected LoomVertex createInput() {
        return new LoomVertex( UUID.randomUUID(), TestDataFactory.entityKey() );
    }

    @Override
    protected LoomVertexStreamSerializer createSerializer() {
        return new LoomVertexStreamSerializer();
    }

}
