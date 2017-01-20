package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.authorization.Principal;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.PrincipalSet;
import com.google.common.collect.Sets;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializer;

@Component
public class PrincipalSetStreamSerializer extends SetStreamSerializer<PrincipalSet, Principal> {

    public PrincipalSetStreamSerializer() {
        super( PrincipalSet.class );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.PRINCIPAL_SET.ordinal();
    }

    @Override
    protected PrincipalSet newInstanceWithExpectedSize( int size ) {
        return PrincipalSet.wrap( Sets.newHashSetWithExpectedSize( size ) );
    }

    @Override
    protected Principal readSingleElement( ObjectDataInput in ) throws IOException {
        return PrincipalStreamSerializer.deserialize( in );
    }

    @Override
    protected void writeSingleElement( ObjectDataOutput out, Principal element ) throws IOException {
        PrincipalStreamSerializer.serialize( out, element );
    }

}
