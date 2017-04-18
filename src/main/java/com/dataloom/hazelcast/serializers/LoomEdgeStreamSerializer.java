package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.core.objects.LoomEdgeKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class LoomEdgeStreamSerializer implements SelfRegisteringStreamSerializer<LoomEdgeKey> {

    @Override
    public Class<? extends LoomEdgeKey> getClazz() {
        return LoomEdgeKey.class;
    }

    @Override
    public void write( ObjectDataOutput out, LoomEdgeKey object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public LoomEdgeKey read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.LOOM_EDGE.ordinal();
    }

    @Override
    public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, LoomEdgeKey object ) throws IOException {
        EdgeKeyStreamSerializer.serialize( out, object.getKey() );
        UUIDStreamSerializer.serialize( out, object.getSrcType() );
        UUIDStreamSerializer.serialize( out, object.getDstType() );
    }

    public static LoomEdgeKey deserialize( ObjectDataInput in ) throws IOException {
        final EdgeKey key = EdgeKeyStreamSerializer.deserialize( in );
        final UUID srcType = UUIDStreamSerializer.deserialize( in );
        final UUID dstType = UUIDStreamSerializer.deserialize( in );
        return new LoomEdgeKey( key, srcType, dstType );
    }

}
