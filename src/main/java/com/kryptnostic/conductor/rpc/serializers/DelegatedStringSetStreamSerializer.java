package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.collect.Sets;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializer;

public class DelegatedStringSetStreamSerializer extends SetStreamSerializer<DelegatedStringSet, String> {

    protected DelegatedStringSetStreamSerializer( Class<DelegatedStringSet> clazz ) {
        super( clazz );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.STRING_SET.ordinal();
    }

    @Override
    protected DelegatedStringSet newInstanceWithExpectedSize( int size ) {
        return new DelegatedStringSet( Sets.newHashSetWithExpectedSize( size ) );
    }

    @Override
    protected String readSingleElement( ObjectDataInput in ) throws IOException {
        return in.readUTF();
    }

    @Override
    protected void writeSingleElement( ObjectDataOutput out, String element ) throws IOException {
        out.writeUTF( element );
    }

}
