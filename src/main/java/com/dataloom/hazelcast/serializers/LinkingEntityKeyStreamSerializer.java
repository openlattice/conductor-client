package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.openlattice.data.EntityKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.LinkingEntityKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class LinkingEntityKeyStreamSerializer implements SelfRegisteringStreamSerializer<LinkingEntityKey> {

    @Override
    public void write( ObjectDataOutput out, LinkingEntityKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        EntityKeyStreamSerializer.serialize( out, object.getEntityKey() );
    }

    @Override
    public LinkingEntityKey read( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );
        EntityKey ek = EntityKeyStreamSerializer.deserialize( in );
        return new LinkingEntityKey( graphId, ek );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.LINKING_ENTITY_KEY.ordinal();

    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends LinkingEntityKey> getClazz() {
        return LinkingEntityKey.class;
    }

}
