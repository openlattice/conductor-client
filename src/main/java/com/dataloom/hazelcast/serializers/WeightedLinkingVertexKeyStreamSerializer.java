package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.WeightedLinkingVertexKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class WeightedLinkingVertexKeyStreamSerializer
        implements SelfRegisteringStreamSerializer<WeightedLinkingVertexKey> {
    @Override public Class<? extends WeightedLinkingVertexKey> getClazz() {
        return WeightedLinkingVertexKey.class;
    }

    @Override public void write( ObjectDataOutput out, WeightedLinkingVertexKey object ) throws IOException {
        serialize( out, object );
    }

    @Override public WeightedLinkingVertexKey read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.WEIGHTED_LINKING_VERTEX_KEY.ordinal();
    }

    @Override public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, WeightedLinkingVertexKey object ) throws IOException {
        out.writeDouble( object.getWeight() );
        LinkingVertexKeyStreamSerializer.serialize( out, object.getVertexKey() );
    }

    public static WeightedLinkingVertexKey deserialize( ObjectDataInput in ) throws IOException {
        double weight = in.readDouble();
        LinkingVertexKey key = LinkingVertexKeyStreamSerializer.deserialize( in );
        return new WeightedLinkingVertexKey( weight, key );
    }
}
