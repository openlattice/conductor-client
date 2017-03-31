package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.graph.core.objects.VertexLabel;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class LoomVertexStreamSerializer implements SelfRegisteringStreamSerializer<LoomVertex> {

    @Override
    public Class<? extends LoomVertex> getClazz() {
        return LoomVertex.class;
    }

    @Override
    public void write( ObjectDataOutput out, LoomVertex object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public LoomVertex read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.LOOM_VERTEX.ordinal();
    }

    @Override
    public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, LoomVertex object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        UUIDStreamSerializer.serialize( out, object.getKey() );
        VertexLabelStreamSerializer.serialize( out, object.getLabel() );
    }

    public static LoomVertex deserialize( ObjectDataInput in ) throws IOException {
        final UUID graphId = UUIDStreamSerializer.deserialize( in );
        final UUID key = UUIDStreamSerializer.deserialize( in );
        final VertexLabel label = VertexLabelStreamSerializer.deserialize( in );
        return new LoomVertex( graphId, key, label );
    }

}
