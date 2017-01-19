package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import com.dataloom.authorization.DelegatedPermissionSet;
import com.dataloom.authorization.Permission;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.collect.Sets;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializer;

public class DelegatedPermissionSetStreamSerializer extends SetStreamSerializer<DelegatedPermissionSet, Permission> {

    private static final Permission[] permissions = Permission.values();

    protected DelegatedPermissionSetStreamSerializer( Class<DelegatedPermissionSet> clazz ) {
        super( clazz );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.STRING_SET.ordinal();
    }

    @Override
    protected DelegatedPermissionSet newInstanceWithExpectedSize( int size ) {
        return new DelegatedPermissionSet( Sets.newHashSetWithExpectedSize( size ) );
    }

    @Override
    protected Permission readSingleElement( ObjectDataInput in ) throws IOException {
        return permissions[ in.readInt() ];
    }

    @Override
    protected void writeSingleElement( ObjectDataOutput out, Permission element ) throws IOException {
        out.writeInt( element.ordinal() );
    }

}
