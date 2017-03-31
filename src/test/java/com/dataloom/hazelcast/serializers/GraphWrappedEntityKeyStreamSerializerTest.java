package com.dataloom.hazelcast.serializers;

import java.io.Serializable;
import java.util.UUID;

import com.dataloom.graph.core.objects.GraphWrappedEntityKey;
import com.dataloom.mapstores.TestDataFactory;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class GraphWrappedEntityKeyStreamSerializerTest
        extends AbstractStreamSerializerTest<GraphWrappedEntityKeyStreamSerializer, GraphWrappedEntityKey>
        implements Serializable {
    private static final long serialVersionUID = 3944632523295596892L;

    @Override
    protected GraphWrappedEntityKey createInput() {
        return new GraphWrappedEntityKey( UUID.randomUUID(), TestDataFactory.entityKey() );
    }

    @Override
    protected GraphWrappedEntityKeyStreamSerializer createSerializer() {
        return new GraphWrappedEntityKeyStreamSerializer();
    }

}
