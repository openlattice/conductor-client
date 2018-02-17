package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.openlattice.edm.set.EntitySetPropertyKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EntitySetPropertyKeyStreamSerializer implements SelfRegisteringStreamSerializer<EntitySetPropertyKey> {

    @Override
    public void write( ObjectDataOutput out, EntitySetPropertyKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getEntitySetId() );
        UUIDStreamSerializer.serialize( out, object.getPropertyTypeId() );
    }

    @Override
    public EntitySetPropertyKey read( ObjectDataInput in ) throws IOException {
        UUID entitySetId = UUIDStreamSerializer.deserialize( in );
        UUID propertyTypeId = UUIDStreamSerializer.deserialize( in );
        return new EntitySetPropertyKey( entitySetId, propertyTypeId );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_SET_PROPERTY_KEY.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends EntitySetPropertyKey> getClazz() {
        return EntitySetPropertyKey.class;
    }

}
