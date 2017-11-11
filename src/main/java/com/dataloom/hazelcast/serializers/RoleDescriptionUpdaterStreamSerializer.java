package com.dataloom.hazelcast.serializers;

import com.dataloom.organizations.roles.processors.PrincipalDescriptionUpdater;
import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class RoleDescriptionUpdaterStreamSerializer implements SelfRegisteringStreamSerializer<PrincipalDescriptionUpdater> {

    @Override
    public void write( ObjectDataOutput out, PrincipalDescriptionUpdater object ) throws IOException {
        out.writeUTF( object.getDescription() );
    }

    @Override
    public PrincipalDescriptionUpdater read( ObjectDataInput in ) throws IOException {
        String newDescription = in.readUTF();
        return new PrincipalDescriptionUpdater( newDescription );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ROLE_DESCRIPTION_UPDATER.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<PrincipalDescriptionUpdater> getClazz() {
        return PrincipalDescriptionUpdater.class;
    }
}