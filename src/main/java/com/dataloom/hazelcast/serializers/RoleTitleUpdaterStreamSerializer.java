package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.roles.processors.PrincipalTitleUpdater;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class RoleTitleUpdaterStreamSerializer implements SelfRegisteringStreamSerializer<PrincipalTitleUpdater> {

    @Override
    public void write( ObjectDataOutput out, PrincipalTitleUpdater object ) throws IOException {
        out.writeUTF( object.getTitle() );
    }

    @Override
    public PrincipalTitleUpdater read( ObjectDataInput in ) throws IOException {
        String newTitle = in.readUTF();
        return new PrincipalTitleUpdater( newTitle );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ROLE_TITLE_UPDATER.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<PrincipalTitleUpdater> getClazz() {
        return PrincipalTitleUpdater.class;
    }
}