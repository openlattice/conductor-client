package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.processors.OrganizationAppMerger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class OrganizationAppMergerStreamSerializer implements SelfRegisteringStreamSerializer<OrganizationAppMerger> {
    @Override public Class<? extends OrganizationAppMerger> getClazz() {
        return OrganizationAppMerger.class;
    }

    @Override public void write(
            ObjectDataOutput out, OrganizationAppMerger object ) throws IOException {
        SetStreamSerializers.serialize(
                out,
                object.getBackingCollection(),
                elem -> UUIDStreamSerializer.serialize( out, elem ) );
    }

    @Override public OrganizationAppMerger read( ObjectDataInput in ) throws IOException {
        return new OrganizationAppMerger(
                SetStreamSerializers.deserialize( in, UUIDStreamSerializer::deserialize )
        );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ORGANIZATION_APP_MERGER.ordinal();
    }

    @Override public void destroy() {

    }
}
