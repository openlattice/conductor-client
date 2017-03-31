package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.objects.GraphWrappedEntityKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class GraphWrappedEntityKeyStreamSerializer implements SelfRegisteringStreamSerializer<GraphWrappedEntityKey> {

    @Override
    public Class<? extends GraphWrappedEntityKey> getClazz() {
        return GraphWrappedEntityKey.class;
    }

    @Override
    public void write( ObjectDataOutput out, GraphWrappedEntityKey object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public GraphWrappedEntityKey read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.GRAPH_WRAPPED_ENTITY_KEY.ordinal();
    }

    @Override
    public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, GraphWrappedEntityKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        EntityKeyStreamSerializer.serialize( out, object.getEntityKey() );
    }

    public static GraphWrappedEntityKey deserialize( ObjectDataInput in ) throws IOException {
        final UUID graphId = UUIDStreamSerializer.deserialize( in );
        final EntityKey entityKey = EntityKeyStreamSerializer.deserialize( in );
        return new GraphWrappedEntityKey( graphId, entityKey );
    }

}
