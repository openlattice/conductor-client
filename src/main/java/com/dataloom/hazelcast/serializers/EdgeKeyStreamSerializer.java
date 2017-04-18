package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EdgeKeyStreamSerializer implements SelfRegisteringStreamSerializer<EdgeKey> {

    @Override
    public Class<? extends EdgeKey> getClazz() {
        return EdgeKey.class;
    }

    @Override
    public void write( ObjectDataOutput out, EdgeKey object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public EdgeKey read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.EDGE_KEY.ordinal();
    }

    @Override
    public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, EdgeKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getSrcId() );
        UUIDStreamSerializer.serialize( out, object.getDstId() );
        EntityKeyStreamSerializer.serialize( out, object.getReference() );
    }

    public static EdgeKey deserialize( ObjectDataInput in ) throws IOException {
        final UUID srcId = UUIDStreamSerializer.deserialize( in );
        final UUID dstId = UUIDStreamSerializer.deserialize( in );
        final EntityKey reference = EntityKeyStreamSerializer.deserialize( in );
        return new EdgeKey( srcId, dstId, reference );
    }

}
