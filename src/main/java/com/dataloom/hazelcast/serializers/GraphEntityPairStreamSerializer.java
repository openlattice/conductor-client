package com.dataloom.hazelcast.serializers;

import com.dataloom.blocking.GraphEntityPair;
import com.dataloom.data.EntityKey;
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
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        EntityKeyStreamSerializer.serialize( out, object.getEntityKey() );
    }

    @Override public GraphEntityPair read( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );
        EntityKey entityKey = EntityKeyStreamSerializer.deserialize( in );
        return new GraphEntityPair( graphId, entityKey );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.GRAPH_ENTITY_PAIR.ordinal();
    }

    @Override public void destroy() {

    }
}
