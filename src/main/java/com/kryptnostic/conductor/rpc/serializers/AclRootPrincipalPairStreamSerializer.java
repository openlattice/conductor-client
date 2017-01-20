package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.dataloom.authorization.Principal;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.requests.mapstores.AclRootPrincipalPair;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class AclRootPrincipalPairStreamSerializer implements SelfRegisteringStreamSerializer<AclRootPrincipalPair> {
    @Override
    public void write( ObjectDataOutput out, AclRootPrincipalPair object )
            throws IOException {
        StreamSerializerUtils.serializeFromList( out, object.getAclRoot(), ( UUID key ) -> {
            UUIDStreamSerializer.serialize( out, key );
        } );
        PrincipalStreamSerializer.serialize( out, object.getUser() );
    }

    @Override
    public AclRootPrincipalPair read( ObjectDataInput in ) throws IOException {
        List<UUID> aclRoot = StreamSerializerUtils.deserializeToList( in, ( ObjectDataInput dataInput ) -> {
            return UUIDStreamSerializer.deserialize( dataInput );
        } );
        Principal user = PrincipalStreamSerializer.deserialize( in );
        return new AclRootPrincipalPair( aclRoot, user );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ACLROOT_PRINCIPAL_PAIR.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<AclRootPrincipalPair> getClazz() {
        return AclRootPrincipalPair.class;
    }

}
