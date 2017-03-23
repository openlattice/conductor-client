package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.roles.processors.RoleTitleUpdater;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class RoleTitleUpdaterStreamSerializer implements SelfRegisteringStreamSerializer<RoleTitleUpdater> {

    @Override
    public void write( ObjectDataOutput out, RoleTitleUpdater object ) throws IOException {
        out.writeUTF( object.getTitle() );
    }

    @Override
    public RoleTitleUpdater read( ObjectDataInput in ) throws IOException {
        String newTitle = in.readUTF();
        return new RoleTitleUpdater( newTitle );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ROLE_TITLE_UPDATER.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<RoleTitleUpdater> getClazz() {
        return RoleTitleUpdater.class;
    }
}