package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.LinkingVertex;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

@Component
public class LinkingVertexStreamSerializer implements SelfRegisteringStreamSerializer<LinkingVertex> {

    @Override
    public void write( ObjectDataOutput out, LinkingVertex object ) throws IOException {
        out.writeDouble( object.getDiameter() );
        SetStreamSerializers.serialize( out, object.getEntityKeys(), ( UUID ek ) -> {
            UUIDStreamSerializer.serialize( out, ek );
        } );
    }

    @Override
    public LinkingVertex read( ObjectDataInput in ) throws IOException {
        double diameter = in.readDouble();
        Set<UUID> entityKeys = SetStreamSerializers.deserialize( in, UUIDStreamSerializer::deserialize );
        return new LinkingVertex( diameter, entityKeys );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.LINKING_VERTEX.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends LinkingVertex> getClazz() {
        return LinkingVertex.class;
    }

}
