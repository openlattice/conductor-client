package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class AssociationTypeStreamSerializer implements SelfRegisteringStreamSerializer<AssociationType> {

    @Override
    public void write( ObjectDataOutput out, AssociationType object ) throws IOException {
        SetStreamSerializers.serialize( out, object.getSrc(), ( UUID key ) -> {
            UUIDStreamSerializer.serialize( out, key );
        } );
        SetStreamSerializers.serialize( out, object.getDst(), ( UUID property ) -> {
            UUIDStreamSerializer.serialize( out, property );
        } );
        out.writeBoolean( object.isBidirectional() );

        EntityType maybeEntityType = object.getAssociationEntityType();
        if ( maybeEntityType != null ) {
            out.writeBoolean( true );
            EntityTypeStreamSerializer.serialize( out, maybeEntityType );
        } else {
            out.writeBoolean( false );
        }
    }

    @Override
    public AssociationType read( ObjectDataInput in ) throws IOException {
        LinkedHashSet<UUID> src = SetStreamSerializers.orderedDeserialize( in, UUIDStreamSerializer::deserialize );
        LinkedHashSet<UUID> dst = SetStreamSerializers.orderedDeserialize( in, UUIDStreamSerializer::deserialize );
        boolean bidirectional = in.readBoolean();
        Optional<EntityType> entityType = Optional.absent();
        if ( in.readBoolean() ) entityType = Optional.of( EntityTypeStreamSerializer.deserialize( in ) );

        return new AssociationType( entityType, src, dst, bidirectional );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ASSOCIATION_TYPE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends AssociationType> getClazz() {
        return AssociationType.class;
    }

}
