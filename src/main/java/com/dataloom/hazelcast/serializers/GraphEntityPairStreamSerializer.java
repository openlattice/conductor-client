package com.dataloom.hazelcast.serializers;

import com.dataloom.blocking.GraphEntityPair;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component
public class GraphEntityPairStreamSerializer implements SelfRegisteringStreamSerializer<GraphEntityPair> {
    @Override public Class<? extends GraphEntityPair> getClazz() {
        return GraphEntityPair.class;
    }

    @Override public void write( ObjectDataOutput out, GraphEntityPair object ) throws IOException {
        serialize( out, object );
    }

    @Override public GraphEntityPair read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    public static void serialize( ObjectDataOutput out, GraphEntityPair object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        UUIDStreamSerializer.serialize( out, object.getEntityKeyId() );
    }

    public static GraphEntityPair deserialize( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );
        UUID entityKeyId = UUIDStreamSerializer.deserialize( in );
        return new GraphEntityPair( graphId, entityKeyId );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.GRAPH_ENTITY_PAIR.ordinal();
    }

    @Override public void destroy() {

    }
}
