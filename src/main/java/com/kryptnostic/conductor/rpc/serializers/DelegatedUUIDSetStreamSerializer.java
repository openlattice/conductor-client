package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.UUID;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.collect.Sets;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDSet;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializer;

public class DelegatedUUIDSetStreamSerializer extends SetStreamSerializer<DelegatedUUIDSet, UUID> {

    public DelegatedUUIDSetStreamSerializer() {
        super( DelegatedUUIDSet.class );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.UUID_SET.ordinal();
    }

    @Override
    protected DelegatedUUIDSet newInstanceWithExpectedSize( int size ) {
        return DelegatedUUIDSet.wrap( Sets.newHashSetWithExpectedSize( size ) );
    }

    @Override
    protected UUID readSingleElement( ObjectDataInput in ) throws IOException {
        return UUIDStreamSerializer.deserialize( in );
    }

    @Override
    protected void writeSingleElement( ObjectDataOutput out, UUID element ) throws IOException {
        UUIDStreamSerializer.serialize( out, element );
    }

}
