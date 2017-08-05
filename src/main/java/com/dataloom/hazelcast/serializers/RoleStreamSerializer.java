package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organization.roles.Role;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class RoleStreamSerializer implements SelfRegisteringStreamSerializer<Role> {

    @Override
    public void write( ObjectDataOutput out, Role object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        UUIDStreamSerializer.serialize( out, object.getOrganizationId() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
    }

    @Override
    public Role read( ObjectDataInput in ) throws IOException {
        Optional<UUID> roleId = Optional.of( UUIDStreamSerializer.deserialize( in ) );
        UUID organizationId = UUIDStreamSerializer.deserialize( in );
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        return new Role( roleId, organizationId, title, description );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ROLE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends Role> getClazz() {
        return Role.class;
    }

}
