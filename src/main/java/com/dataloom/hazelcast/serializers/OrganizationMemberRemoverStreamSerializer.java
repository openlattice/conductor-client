package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.processors.OrganizationMemberRemover;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class OrganizationMemberRemoverStreamSerializer
        implements SelfRegisteringStreamSerializer<OrganizationMemberRemover> {

    @Override
    public Class<OrganizationMemberRemover> getClazz() {
        return OrganizationMemberRemover.class;
    }

    @Override
    public void write( ObjectDataOutput out, OrganizationMemberRemover object ) throws IOException {
        SetStreamSerializers.serialize(
                out,
                object.getBackingCollection(),
                elem -> PrincipalStreamSerializer.serialize( out, elem )
        );
    }

    @Override
    public OrganizationMemberRemover read( ObjectDataInput in ) throws IOException {
        return new OrganizationMemberRemover(
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
