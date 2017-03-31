package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.GraphWrappedEdgeKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class GraphWrappedEdgeKeyStreamSerializer implements SelfRegisteringStreamSerializer<GraphWrappedEdgeKey> {

    @Override
    public Class<? extends GraphWrappedEdgeKey> getClazz() {
        return GraphWrappedEdgeKey.class;
    }

    @Override
    public void write( ObjectDataOutput out, GraphWrappedEdgeKey object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public GraphWrappedEdgeKey read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.GRAPH_WRAPPED_EDGE_KEY.ordinal();
    }

    @Override
    public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, GraphWrappedEdgeKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        EdgeKeyStreamSerializer.serialize( out, object.getEdgeKey() );
    }

    public static GraphWrappedEdgeKey deserialize( ObjectDataInput in ) throws IOException {
        final UUID graphId = UUIDStreamSerializer.deserialize( in );
        final EdgeKey edgeKey = EdgeKeyStreamSerializer.deserialize( in );
        return new GraphWrappedEdgeKey( graphId, edgeKey );
    }

}
