package com.dataloom.hazelcast.serializers;

import com.dataloom.organization.roles.Role;
import org.springframework.stereotype.Component;

@Component
public class RoleStreamSerializer extends SecurablePrincipalStreamSerializer {
    public RoleStreamSerializer() {
        super( Role.class );
    }

}
