package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class LoomEdgeStreamSerializer implements SelfRegisteringStreamSerializer<LoomEdge> {

    @Override
    public Class<? extends LoomEdge> getClazz() {
        return LoomEdge.class;
    }

    @Override
    public void write( ObjectDataOutput out, LoomEdge object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public LoomEdge read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.LOOM_EDGE.ordinal();
    }

    @Override
    public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, LoomEdge object ) throws IOException {
        EdgeKeyStreamSerializer.serialize( out, object.getKey() );
        EntityKeyStreamSerializer.serialize( out, object.getReference() );
        UUIDStreamSerializer.serialize( out, object.getSrcType() );
        UUIDStreamSerializer.serialize( out, object.getDstType() );
    }

    public static LoomEdge deserialize( ObjectDataInput in ) throws IOException {
        final EdgeKey key = EdgeKeyStreamSerializer.deserialize( in );
        final EntityKey reference = EntityKeyStreamSerializer.deserialize( in );
        final UUID srcType = UUIDStreamSerializer.deserialize( in );
        final UUID dstType = UUIDStreamSerializer.deserialize( in );
        return new LoomEdge( key, reference, srcType, dstType );
    }

}
