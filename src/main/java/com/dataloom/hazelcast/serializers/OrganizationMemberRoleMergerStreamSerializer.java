package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.processors.OrganizationMemberRoleMerger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class OrganizationMemberRoleMergerStreamSerializer
        implements SelfRegisteringStreamSerializer<OrganizationMemberRoleMerger> {

    @Override
    public Class<OrganizationMemberRoleMerger> getClazz() {
        return OrganizationMemberRoleMerger.class;
    }

    @Override
    public void write( ObjectDataOutput out, OrganizationMemberRoleMerger object ) throws IOException {
        SetStreamSerializers.serialize(
                out,
                object.getBackingCollection(),
                elem -> PrincipalStreamSerializer.serialize( out, elem )
        );
    }

    @Override
    public OrganizationMemberRoleMerger read( ObjectDataInput in ) throws IOException {
        return new OrganizationMemberRoleMerger(
                SetStreamSerializers.deserialize( in, PrincipalStreamSerializer::deserialize )
        );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ORGANIZATION_MEMBER_MERGER.ordinal();
    }

    @Override
    public void destroy() {
    }
}
