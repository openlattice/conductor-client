package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.UUID;

import com.dataloom.graph.core.mapstores.EdgesMapstore;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.mapstores.TestDataFactory;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class LoomEdgeStreamSerializerTest extends AbstractStreamSerializerTest<LoomEdgeStreamSerializer, LoomEdge>
        implements Serializable {

    private static final long serialVersionUID = -46668929768747463L;

    @Override
    protected LoomEdge createInput() {
        return new LoomEdge(
                EdgesMapstore.generateTestEdgeKey(),
                TestDataFactory.entityKey(),
                UUID.randomUUID(),
                UUID.randomUUID() );
    }

    @Override
    protected LoomEdgeStreamSerializer createSerializer() {
        return new LoomEdgeStreamSerializer();
    }

}
