package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.processors.OrganizationAppRemover;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class OrganizationAppRemoverStreamSerializer implements SelfRegisteringStreamSerializer<OrganizationAppRemover> {
    @Override public Class<? extends OrganizationAppRemover> getClazz() {
        return OrganizationAppRemover.class;
    }

    @Override public void write(
            ObjectDataOutput out, OrganizationAppRemover object ) throws IOException {
        SetStreamSerializers.serialize(
                out,
                object.getBackingCollection(),
                elem -> UUIDStreamSerializer.serialize( out, elem )
        );
    }

    @Override public OrganizationAppRemover read( ObjectDataInput in ) throws IOException {
        return new OrganizationAppRemover(
                SetStreamSerializers.deserialize( in, UUIDStreamSerializer::deserialize )
        );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ORGANIZATION_APP_REMOVER.ordinal();
    }

    @Override public void destroy() {

    }
}
