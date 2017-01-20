package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.Principal;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class AceKeyStreamSerializer implements SelfRegisteringStreamSerializer<AceKey> {

    @Override
    public void write( ObjectDataOutput out, AceKey object )
            throws IOException {
        StreamSerializerUtils.serializeFromList( out, object.getKey(), ( UUID key ) -> {
            UUIDStreamSerializer.serialize( out, key );
        } );
        PrincipalStreamSerializer.serialize( out, object.getPrincipal() );
    }

    @Override
    public AceKey read( ObjectDataInput in ) throws IOException {
        List<UUID> keys = StreamSerializerUtils.deserializeToList( in, ( ObjectDataInput dataInput ) -> {
            return UUIDStreamSerializer.deserialize( dataInput );
        } );
        Principal principal = PrincipalStreamSerializer.deserialize( in );
        return new AceKey( keys, principal );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ACE_KEY.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<AceKey> getClazz() {
        return AceKey.class;
    }

}
