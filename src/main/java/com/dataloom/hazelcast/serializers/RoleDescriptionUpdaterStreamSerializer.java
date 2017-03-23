package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.roles.processors.RoleDescriptionUpdater;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class RoleDescriptionUpdaterStreamSerializer implements SelfRegisteringStreamSerializer<RoleDescriptionUpdater> {

    @Override
    public void write( ObjectDataOutput out, RoleDescriptionUpdater object ) throws IOException {
        out.writeUTF( object.getDescription() );
    }

    @Override
    public RoleDescriptionUpdater read( ObjectDataInput in ) throws IOException {
        String newDescription = in.readUTF();
        return new RoleDescriptionUpdater( newDescription );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ROLE_DESCRIPTION_UPDATER.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<RoleDescriptionUpdater> getClazz() {
        return RoleDescriptionUpdater.class;
    }
}