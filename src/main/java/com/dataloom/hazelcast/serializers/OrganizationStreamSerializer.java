package com.dataloom.hazelcast.serializers;

import com.openlattice.authorization.Principal;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.openlattice.organization.Organization;
import com.openlattice.organization.OrganizationPrincipal;
import com.openlattice.organization.roles.Role;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

@Component
public class OrganizationStreamSerializer implements SelfRegisteringStreamSerializer<Organization> {
    @Override public Class<? extends Organization> getClazz() {
        return Organization.class;
    }

    @Override public void write( ObjectDataOutput out, Organization object ) throws IOException {
        serialize( out, object );
    }

    @Override public Organization read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ORGANIZATION.ordinal();
    }

    @Override public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, Organization object ) throws IOException {
        SecurablePrincipalStreamSerializer.serialize( out, object.getSecurablePrincipal() );
        SetStreamSerializers.fastStringSetSerialize( out, object.getAutoApprovedEmails() );
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getApps() );
        SetStreamSerializers.serialize( out, object.getMembers(), (principal) -> {
            PrincipalStreamSerializer.serialize( out, principal );
        } );
        SetStreamSerializers.serialize( out, object.getRoles(), (role) -> {
            RoleStreamSerializer.serialize( out, role );
        } );
    }

    public static Organization deserialize( ObjectDataInput in ) throws IOException {
        OrganizationPrincipal securablePrincipal = (OrganizationPrincipal) SecurablePrincipalStreamSerializer.deserialize( in );
        Set<String> autoApprovedEmails = SetStreamSerializers.fastStringSetDeserialize( in );
        Set<UUID> apps = SetStreamSerializers.fastUUIDSetDeserialize( in );
        Set<Principal> members = SetStreamSerializers.deserialize( in, PrincipalStreamSerializer::deserialize );
        Set<Role> roles = SetStreamSerializers.deserialize( in, RoleStreamSerializer::deserialize );

        return new Organization( securablePrincipal, autoApprovedEmails, members, roles, apps );
    }
}
