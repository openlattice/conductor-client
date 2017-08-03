package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.processors.OrganizationMemberMerger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class OrganizationMemberMergerStreamSerializer
        implements SelfRegisteringStreamSerializer<OrganizationMemberMerger> {

    @Override
    public Class<OrganizationMemberMerger> getClazz() {
        return OrganizationMemberMerger.class;
    }

    @Override
    public void write( ObjectDataOutput out, OrganizationMemberMerger object ) throws IOException {
        SetStreamSerializers.serialize(
                out,
                object.getBackingCollection(),
                elem -> PrincipalStreamSerializer.serialize( out, elem )
        );
    }

    @Override
    public OrganizationMemberMerger read( ObjectDataInput in ) throws IOException {
        return new OrganizationMemberMerger(
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
