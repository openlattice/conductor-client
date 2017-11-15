package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organization.roles.Role;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class RoleStreamSerializer implements SelfRegisteringStreamSerializer<Role> {
    @Override public Class<? extends Role> getClazz() {
        return Role.class;
    }

    @Override public void write( ObjectDataOutput out, Role object ) throws IOException {
        serialize( out, object );
    }

    @Override public Role read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ROLE.ordinal();
    }

    @Override public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, Role object ) throws IOException {
        SecurablePrincipalStreamSerializer.serialize( out, object );
    }

    public static Role deserialize( ObjectDataInput in ) throws IOException {
        //TODO: Split up securable principal stream serializer
        return (Role) SecurablePrincipalStreamSerializer.deserialize( in );
    }
}
