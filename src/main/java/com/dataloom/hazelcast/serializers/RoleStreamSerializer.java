package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organization.roles.Role;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class RoleStreamSerializer implements SelfRegisteringStreamSerializer<Role> {
    @Override public Class<? extends Role> getClazz() {
        return Role.class;
    }

    @Override public void write( ObjectDataOutput out, Role object ) throws IOException {
        SecurablePrincipalStreamSerializer.serialize( out, object );
    }

    @Override public Role read( ObjectDataInput in ) throws IOException {
        //TODO: Split up securable principal stream serializer
        return (Role) SecurablePrincipalStreamSerializer.deserialize( in );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ROLE.ordinal();
    }

    @Override public void destroy() {

    }
}
