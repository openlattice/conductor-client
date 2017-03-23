package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organization.roles.OrganizationRole;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class OrganizationRoleStreamSerializer implements SelfRegisteringStreamSerializer<OrganizationRole> {

    @Override
    public void write( ObjectDataOutput out, OrganizationRole object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        UUIDStreamSerializer.serialize( out, object.getOrganizationId() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
    }

    @Override
    public OrganizationRole read( ObjectDataInput in ) throws IOException {
        Optional<UUID> roleId = Optional.of( UUIDStreamSerializer.deserialize( in ) );
        UUID organizationId = UUIDStreamSerializer.deserialize( in );
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        return new OrganizationRole( roleId, organizationId, title, description );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ORGANIZATION_ROLE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends OrganizationRole> getClazz() {
        return OrganizationRole.class;
    }

}
