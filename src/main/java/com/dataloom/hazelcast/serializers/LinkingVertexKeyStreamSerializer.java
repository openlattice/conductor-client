package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.LinkingVertexKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class LinkingVertexKeyStreamSerializer implements SelfRegisteringStreamSerializer<LinkingVertexKey> {

    @Override
    public void write( ObjectDataOutput out, LinkingVertexKey object ) throws IOException {
        serialize( out, object ); 
    }

    @Override
    public LinkingVertexKey read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.LINKING_VERTEX_KEY.ordinal();

    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends LinkingVertexKey> getClazz() {
        return LinkingVertexKey.class;
    }
    
    public static void serialize( ObjectDataOutput out, LinkingVertexKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() ); 
        UUIDStreamSerializer.serialize( out, object.getVertexId() ); 
    }
    
    public static LinkingVertexKey deserialize( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );
        UUID vertexId = UUIDStreamSerializer.deserialize( in );
        return new LinkingVertexKey( graphId, vertexId );
    }

}
