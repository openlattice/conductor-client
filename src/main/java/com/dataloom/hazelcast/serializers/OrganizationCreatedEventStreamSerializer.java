package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organization.Organization;
import com.dataloom.organizations.events.OrganizationCreatedEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class OrganizationCreatedEventStreamSerializer implements
        SelfRegisteringStreamSerializer<OrganizationCreatedEvent> {
    @Override public Class<? extends OrganizationCreatedEvent> getClazz() {
        return OrganizationCreatedEvent.class;
    }

    @Override public void write(
            ObjectDataOutput out, OrganizationCreatedEvent object ) throws IOException {
        OrganizationStreamSerializer.serialize( out, object.getOrganization() );
    }

    @Override public OrganizationCreatedEvent read( ObjectDataInput in ) throws IOException {
        Organization organization = OrganizationStreamSerializer.deserialize( in );
        return new OrganizationCreatedEvent( organization );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ORGANIZATION_CREATED_EVENT.ordinal();
    }

    @Override public void destroy() {

    }
}
