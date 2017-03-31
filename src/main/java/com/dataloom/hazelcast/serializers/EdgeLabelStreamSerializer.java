package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeLabel;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EdgeLabelStreamSerializer implements SelfRegisteringStreamSerializer<EdgeLabel> {

    @Override
    public Class<? extends EdgeLabel> getClazz() {
        return EdgeLabel.class;
    }

    @Override
    public void write( ObjectDataOutput out, EdgeLabel object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public EdgeLabel read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.EDGE_LABEL.ordinal();
    }

    @Override
    public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, EdgeLabel object ) throws IOException {
        EntityKeyStreamSerializer.serialize( out, object.getReference() );
        UUIDStreamSerializer.serialize( out, object.getSrcType() );
        UUIDStreamSerializer.serialize( out, object.getDstType() );
    }

    public static EdgeLabel deserialize( ObjectDataInput in ) throws IOException {
        final EntityKey reference = EntityKeyStreamSerializer.deserialize( in );
        final UUID srcType = UUIDStreamSerializer.deserialize( in );
        final UUID dstType = UUIDStreamSerializer.deserialize( in );

        return new EdgeLabel( reference, srcType, dstType );
    }

}
