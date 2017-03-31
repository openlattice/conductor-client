package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.graph.core.objects.GraphWrappedVertexId;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class GraphWrappedVertexIdStreamSerializer implements SelfRegisteringStreamSerializer<GraphWrappedVertexId> {

    @Override
    public Class<? extends GraphWrappedVertexId> getClazz() {
        return GraphWrappedVertexId.class;
    }

    @Override
    public void write( ObjectDataOutput out, GraphWrappedVertexId object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public GraphWrappedVertexId read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.GRAPH_WRAPPED_VERTEX_ID.ordinal();
    }

    @Override
    public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, GraphWrappedVertexId object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        UUIDStreamSerializer.serialize( out, object.getVertexId() );
    }

    public static GraphWrappedVertexId deserialize( ObjectDataInput in ) throws IOException {
        final UUID graphId = UUIDStreamSerializer.deserialize( in );
        final UUID vertexId = UUIDStreamSerializer.deserialize( in );
        return new GraphWrappedVertexId( graphId, vertexId );
    }

}
