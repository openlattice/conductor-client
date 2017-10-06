package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.aggregators.WeightedLinkingVertexKeyMerger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;

@Component
public class WeightedLinkingVertexKeyMergerStreamSerializer
        implements SelfRegisteringStreamSerializer<WeightedLinkingVertexKeyMerger> {
    @Override public Class<? extends WeightedLinkingVertexKeyMerger> getClazz() {
        return WeightedLinkingVertexKeyMerger.class;
    }

    @Override public void write(
            ObjectDataOutput out, WeightedLinkingVertexKeyMerger object ) throws IOException {
        WeightedLinkingVertexKeyStreamSerializer.serialize( out, object.getObjects().iterator().next() );
    }

    @Override public WeightedLinkingVertexKeyMerger read( ObjectDataInput in ) throws IOException {
        return new WeightedLinkingVertexKeyMerger( Arrays
                .asList( WeightedLinkingVertexKeyStreamSerializer.deserialize( in ) ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.WEIGHTED_LINKING_VERTEX_KEY_MERGER.ordinal();
    }

    @Override public void destroy() {

    }
}
