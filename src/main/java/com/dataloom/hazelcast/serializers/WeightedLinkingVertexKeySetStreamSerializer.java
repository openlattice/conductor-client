package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.WeightedLinkingVertexKey;
import com.dataloom.linking.WeightedLinkingVertexKeySet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class WeightedLinkingVertexKeySetStreamSerializer
        implements SelfRegisteringStreamSerializer<WeightedLinkingVertexKeySet> {
    @Override public Class<? extends WeightedLinkingVertexKeySet> getClazz() {
        return WeightedLinkingVertexKeySet.class;
    }

    @Override public void write( ObjectDataOutput out, WeightedLinkingVertexKeySet object ) throws IOException {
        out.writeInt( object.size() );
        for ( WeightedLinkingVertexKey key : object ) {
            WeightedLinkingVertexKeyStreamSerializer.serialize( out, key );
        }
    }

    @Override public WeightedLinkingVertexKeySet read( ObjectDataInput in ) throws IOException {
        int size = in.readInt();
        WeightedLinkingVertexKeySet result = new WeightedLinkingVertexKeySet();
        for ( int i = 0; i < size; i++ ) {
            result.add( WeightedLinkingVertexKeyStreamSerializer.deserialize( in ) );
        }

        return result;
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.WEIGHTED_LINKING_VERTEX_KEY_SET.ordinal();
    }

    @Override public void destroy() {

    }
}
