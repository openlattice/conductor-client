package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EntitySetStreamSerializer implements SelfRegisteringStreamSerializer<EntitySet> {

    @Override
    public void write( ObjectDataOutput out, EntitySet object )
            throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        UUIDStreamSerializer.serialize( out, object.getEntityTypeId() );
        out.writeUTF( object.getName() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
    }

    @Override
    public EntitySet read( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        UUID entityTypeId = UUIDStreamSerializer.deserialize( in );
        String name = in.readUTF();
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        EntitySet es = new EntitySet(
                id,
                entityTypeId,
                name,
                title,
                description );
        return es;
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_SET.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<EntitySet> getClazz() {
        return EntitySet.class;
    }

}
