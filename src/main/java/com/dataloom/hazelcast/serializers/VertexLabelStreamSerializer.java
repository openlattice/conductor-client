package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.VertexLabel;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class VertexLabelStreamSerializer implements SelfRegisteringStreamSerializer<VertexLabel> {

    @Override
    public Class<? extends VertexLabel> getClazz() {
        return VertexLabel.class;
    }

    @Override
    public void write( ObjectDataOutput out, VertexLabel object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public VertexLabel read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.VERTEX_LABEL.ordinal();
    }

    @Override
    public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, VertexLabel object ) throws IOException {
        EntityKeyStreamSerializer.serialize( out, object.getReference() );
    }

    public static VertexLabel deserialize( ObjectDataInput in ) throws IOException {
        final EntityKey reference = EntityKeyStreamSerializer.deserialize( in );
        return new VertexLabel( reference );
    }

}
