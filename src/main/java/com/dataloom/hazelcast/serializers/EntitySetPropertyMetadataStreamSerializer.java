package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.edm.set.EntitySetPropertyMetadata;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EntitySetPropertyMetadataStreamSerializer
        implements SelfRegisteringStreamSerializer<EntitySetPropertyMetadata> {

    @Override
    public void write( ObjectDataOutput out, EntitySetPropertyMetadata object ) throws IOException {
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        out.writeBoolean( object.getDefaultShow() );
    }

    @Override
    public EntitySetPropertyMetadata read( ObjectDataInput in ) throws IOException {
        String title = in.readUTF();
        String description = in.readUTF();
        boolean defaultShow = in.readBoolean();
        return new EntitySetPropertyMetadata( title, description, defaultShow );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_SET_PROPERTY_METADATA.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends EntitySetPropertyMetadata> getClazz() {
        return EntitySetPropertyMetadata.class;
    }

}
