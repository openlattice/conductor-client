package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.UUID;

import com.dataloom.graph.core.mapstores.EdgesMapstore;
import com.dataloom.graph.core.objects.LoomEdgeKey;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class LoomEdgeKeyStreamSerializerTest extends AbstractStreamSerializerTest<LoomEdgeStreamSerializer, LoomEdgeKey>
        implements Serializable {

    private static final long serialVersionUID = -46668929768747463L;

    @Override
    protected LoomEdgeKey createInput() {
        return new LoomEdgeKey(
                EdgesMapstore.generateTestEdgeKey(),
                UUID.randomUUID(),
                UUID.randomUUID() );
    }

    @Override
    protected LoomEdgeStreamSerializer createSerializer() {
        return new LoomEdgeStreamSerializer();
    }

}
