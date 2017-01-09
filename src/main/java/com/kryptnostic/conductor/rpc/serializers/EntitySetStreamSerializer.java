package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractUUIDStreamSerializer;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class EntitySetStreamSerializer implements SelfRegisteringStreamSerializer<EntitySet> {

    @Override
    public void write( ObjectDataOutput out, EntitySet object )
            throws IOException {
        AbstractUUIDStreamSerializer.serialize( out, object.getId() );
        AbstractUUIDStreamSerializer.serialize( out, object.getEntityTypeId() );
        FullQualifiedNameStreamSerializer.serialize( out, object.getType() );
        out.writeUTF( object.getName() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
    }

    @Override
    public EntitySet read( ObjectDataInput in ) throws IOException {
        UUID id = AbstractUUIDStreamSerializer.deserialize( in );
        UUID entityTypeId = AbstractUUIDStreamSerializer.deserialize( in );
        FullQualifiedName fqn = FullQualifiedNameStreamSerializer.deserialize( in );
        String name = in.readUTF();
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        EntitySet es = new EntitySet(
                id,
                fqn,
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
