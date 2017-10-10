package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.aggregators.WeightedLinkingVertexKeyValueRemover;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;

@Component
public class WeightedLinkingVertexKeyValueRemoverStreamSerializer implements
        SelfRegisteringStreamSerializer<WeightedLinkingVertexKeyValueRemover> {
    @Override public Class<? extends WeightedLinkingVertexKeyValueRemover> getClazz() {
        return WeightedLinkingVertexKeyValueRemover.class;
    }

    @Override public void write(
            ObjectDataOutput out, WeightedLinkingVertexKeyValueRemover object ) throws IOException {
        WeightedLinkingVertexKeyStreamSerializer.serialize( out, object.getObjects().iterator().next() );
    }

    @Override public WeightedLinkingVertexKeyValueRemover read( ObjectDataInput in ) throws IOException {
        return new WeightedLinkingVertexKeyValueRemover( Arrays
                .asList( WeightedLinkingVertexKeyStreamSerializer.deserialize( in ) ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.WEIGHTED_LINKING_VERTEX_KEY_VALUE_REMOVER.ordinal();
    }

    @Override public void destroy() {

    }
}
