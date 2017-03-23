package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organization.roles.RoleKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class RoleKeyStreamSerializer implements SelfRegisteringStreamSerializer<RoleKey>{

    @Override
    public void write( ObjectDataOutput out, RoleKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getOrganizationId() );
        UUIDStreamSerializer.serialize( out, object.getRoleId() );
    }

    @Override
    public RoleKey read( ObjectDataInput in ) throws IOException {
        UUID organizationId = UUIDStreamSerializer.deserialize( in );
        UUID roleId = UUIDStreamSerializer.deserialize( in );
        return new RoleKey( organizationId, roleId );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ROLE_KEY.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends RoleKey> getClazz() {
        return RoleKey.class;
    }

}
