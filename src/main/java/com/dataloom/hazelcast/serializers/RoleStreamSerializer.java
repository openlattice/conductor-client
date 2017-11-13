package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organization.roles.Role;
import org.springframework.stereotype.Component;

@Component
public class RoleStreamSerializer extends SecurablePrincipalStreamSerializer {
    public RoleStreamSerializer() {
        super( Role.class );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ROLE.ordinal();
    }
}
