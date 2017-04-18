package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import com.dataloom.graph.core.objects.LoomVertexKey;
import org.springframework.stereotype.Component;

import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class LoomVertexStreamSerializer implements SelfRegisteringStreamSerializer<LoomVertexKey> {

    @Override
    public Class<? extends LoomVertexKey> getClazz() {
        return LoomVertexKey.class;
    }

    @Override
    public void write( ObjectDataOutput out, LoomVertexKey object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public LoomVertexKey read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.LOOM_VERTEX.ordinal();
    }

    @Override
    public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, LoomVertexKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getKey() );
        EntityKeyStreamSerializer.serialize( out, object.getReference() );
    }

    public static LoomVertexKey deserialize( ObjectDataInput in ) throws IOException {
        final UUID key = UUIDStreamSerializer.deserialize( in );
        final EntityKey reference = EntityKeyStreamSerializer.deserialize( in );
        return new LoomVertexKey( key, reference );
    }

}
