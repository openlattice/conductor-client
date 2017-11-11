package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.processors.NestedPrincipalMerger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class OrganizationMemberRoleMergerStreamSerializer
        implements SelfRegisteringStreamSerializer<NestedPrincipalMerger> {

    @Override
    public Class<NestedPrincipalMerger> getClazz() {
        return NestedPrincipalMerger.class;
    }

    @Override
    public void write( ObjectDataOutput out, NestedPrincipalMerger object ) throws IOException {
        SetStreamSerializers.serialize(
                out,
                object.getBackingCollection(),
                elem -> PrincipalStreamSerializer.serialize( out, elem )
        );
    }

    @Override
    public NestedPrincipalMerger read( ObjectDataInput in ) throws IOException {
        return new NestedPrincipalMerger(
                SetStreamSerializers.deserialize( in, PrincipalStreamSerializer::deserialize )
        );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ORGANIZATION_MEMBER_ROLE_MERGER.ordinal();
    }

    @Override
    public void destroy() {
    }
}
