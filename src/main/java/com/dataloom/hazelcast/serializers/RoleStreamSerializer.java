package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organization.roles.Role;
import com.hazelcast.nio.ObjectDataInput;
import com.openlattice.authorization.SecurablePrincipal;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class RoleStreamSerializer extends SecurablePrincipalStreamSerializer {
    @Override public Class<? extends SecurablePrincipal> getClazz() {
        return Role.class;
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ROLE.ordinal();
    }

    @Override public Role read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    public static Role deserialize( ObjectDataInput in ) throws IOException {
        //TODO: Split up securable principal stream serializer
        return (Role) SecurablePrincipalStreamSerializer.deserialize( in );
    }
}
