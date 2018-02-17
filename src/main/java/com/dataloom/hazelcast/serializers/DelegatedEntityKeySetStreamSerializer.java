package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.data.hazelcast.DelegatedEntityKeySet;
import com.openlattice.data.EntityKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.collect.Sets;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializer;

@Component
public class DelegatedEntityKeySetStreamSerializer extends SetStreamSerializer<DelegatedEntityKeySet, EntityKey> {

    public DelegatedEntityKeySetStreamSerializer( ) {
        super( DelegatedEntityKeySet.class );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_KEY_SET.ordinal();
    }

    @Override
    protected DelegatedEntityKeySet newInstanceWithExpectedSize( int size ) {
        return DelegatedEntityKeySet.wrap( Sets.newHashSetWithExpectedSize( size ) );
    }

    @Override
    protected EntityKey readSingleElement( ObjectDataInput in ) throws IOException {
        return EntityKeyStreamSerializer.deserialize( in );
    }

    @Override
    protected void writeSingleElement( ObjectDataOutput out, EntityKey element ) throws IOException {
        EntityKeyStreamSerializer.serialize( out, element );
    }

}
