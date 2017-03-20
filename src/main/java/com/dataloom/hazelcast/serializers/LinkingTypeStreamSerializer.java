package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import com.dataloom.edm.type.LinkingType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class LinkingTypeStreamSerializer implements SelfRegisteringStreamSerializer<LinkingType> {

    @Override
    public void write( ObjectDataOutput out, LinkingType object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        FullQualifiedNameStreamSerializer.serialize( out, object.getType() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        SetStreamSerializers.serialize( out, object.getSchemas(), ( FullQualifiedName schema ) -> {
            FullQualifiedNameStreamSerializer.serialize( out, schema );
        } );
        UUIDStreamSerializer.serialize( out, object.getSrc() );
        UUIDStreamSerializer.serialize( out, object.getDest() );
        out.writeBoolean( object.isBidirectional() );
    }

    @Override
    public LinkingType read( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        FullQualifiedName type = FullQualifiedNameStreamSerializer.deserialize( in );
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in, ( ObjectDataInput dataInput ) -> {
            return FullQualifiedNameStreamSerializer.deserialize( dataInput );
        } );
        UUID src = UUIDStreamSerializer.deserialize( in );
        UUID dest = UUIDStreamSerializer.deserialize( in );
        boolean bidirectional = in.readBoolean();
        return new LinkingType( id, type, title, description, schemas, src, dest, bidirectional );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.LINKING_TYPE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends LinkingType> getClazz() {
        return LinkingType.class;
    }

}
