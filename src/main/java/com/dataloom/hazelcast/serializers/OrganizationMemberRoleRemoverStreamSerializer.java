package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.processors.NestedPrincipalRemover;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class OrganizationMemberRoleRemoverStreamSerializer
        implements SelfRegisteringStreamSerializer<NestedPrincipalRemover> {

    @Override
    public Class<NestedPrincipalRemover> getClazz() {
        return NestedPrincipalRemover.class;
    }

    @Override
    public void write( ObjectDataOutput out, NestedPrincipalRemover object ) throws IOException {
        SetStreamSerializers.serialize(
                out,
                object.getBackingCollection(),
                elem -> PrincipalStreamSerializer.serialize( out, elem )
        );
    }

    @Override
    public NestedPrincipalRemover read( ObjectDataInput in ) throws IOException {
        return new NestedPrincipalRemover(
                SetStreamSerializers.deserialize( in, PrincipalStreamSerializer::deserialize )
        );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ORGANIZATION_MEMBER_ROLE_REMOVER.ordinal();
    }

    @Override
    public void destroy() {
    }
}
