package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.authorization.Principal;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.requests.mapstores.PrincipalRequestIdPair;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class PrincipalRequestIdPairStreamSerializer implements SelfRegisteringStreamSerializer<PrincipalRequestIdPair> {

    @Override
    public void write( ObjectDataOutput out, PrincipalRequestIdPair object ) throws IOException {
        PrincipalStreamSerializer.serialize( out, object.getUser() );
        UUIDStreamSerializer.serialize( out, object.getRequestId() );
    }

    @Override
    public PrincipalRequestIdPair read( ObjectDataInput in ) throws IOException {
        Principal user = PrincipalStreamSerializer.deserialize( in );
        UUID id = UUIDStreamSerializer.deserialize( in );
        return new PrincipalRequestIdPair( user, id );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.PRINCIPAL_REQUESTID_PAIR.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<PrincipalRequestIdPair> getClazz() {
        return PrincipalRequestIdPair.class;
    }

}
