package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class InitializerAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<HazelcastLinkingGraphs.Initializer> {
    @Override public Class<? extends HazelcastLinkingGraphs.Initializer> getClazz() {
        return HazelcastLinkingGraphs.Initializer.class;
    }

    @Override public void write(
            ObjectDataOutput out, HazelcastLinkingGraphs.Initializer object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
    }

    @Override public HazelcastLinkingGraphs.Initializer read( ObjectDataInput in ) throws IOException {
        return new HazelcastLinkingGraphs.Initializer( UUIDStreamSerializer.deserialize( in ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.INITIALIZER_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {
    }
}
