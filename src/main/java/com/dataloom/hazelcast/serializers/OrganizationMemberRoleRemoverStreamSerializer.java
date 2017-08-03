package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.processors.OrganizationMemberRoleRemover;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class OrganizationMemberRoleRemoverStreamSerializer
        implements SelfRegisteringStreamSerializer<OrganizationMemberRoleRemover> {

    @Override
    public Class<OrganizationMemberRoleRemover> getClazz() {
        return OrganizationMemberRoleRemover.class;
    }

    @Override
    public void write( ObjectDataOutput out, OrganizationMemberRoleRemover object ) throws IOException {
        SetStreamSerializers.serialize(
                out,
                object.getBackingCollection(),
                elem -> PrincipalStreamSerializer.serialize( out, elem )
        );
    }

    @Override
    public OrganizationMemberRoleRemover read( ObjectDataInput in ) throws IOException {
        return new OrganizationMemberRoleRemover(
                SetStreamSerializers.deserialize( in, PrincipalStreamSerializer::deserialize )
        );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ORGANIZATION_MEMBER_REMOVER.ordinal();
    }

    @Override
    public void destroy() {
    }
}
