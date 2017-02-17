package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.LinkingVertex;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class LinkingVertexStreamSerializer implements SelfRegisteringStreamSerializer<LinkingVertex> {

    @Override
    public void write( ObjectDataOutput out, LinkingVertex object ) throws IOException {
        out.writeDouble( object.getDiameter() );
        SetStreamSerializers.serialize( out, object.getEntityKeys(), ( EntityKey ek ) -> {
            EntityKeyStreamSerializer.serialize( out, ek );
        } );
    }

    @Override
    public LinkingVertex read( ObjectDataInput in ) throws IOException {
        double diameter = in.readDouble();
        Set<EntityKey> entityKeys = SetStreamSerializers.deserialize( in,  EntityKeyStreamSerializer::deserialize );
        return new LinkingVertex( diameter, entityKeys );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.LINKING_VERTEX.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends LinkingVertex> getClazz() {
        return LinkingVertex.class;
    }

}
